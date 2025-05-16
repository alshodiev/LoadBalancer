import asyncio
import json
import logging
import time
import random
from typing import Tuple, Optional, Dict, Any

from rpc import MessageType, send_message, read_message
from config import (
    WORKER_METRICS_INTERVAL_SECONDS,
)

class Worker:
    def __init__(self, worker_id: str,
                 # Worker's own RPC server settings (where it listens for proxy's requests)
                 # This is actually not used if bidirectional comms on worker->proxy connection.
                 # Kept for potential future use if proxy initiates connections to worker.
                 rpc_listen_host: str, rpc_listen_port: int,
                 # Proxy's internal RPC server settings (where worker connects to register/send metrics)
                 proxy_rpc_host: str, proxy_rpc_port: int,
                 sim_delay_range_ms: Tuple[int, int],
                 performance_factor: float = 1.0):
        self.worker_id = worker_id
        # Info about where this worker *would* listen if proxy connected to it.
        # In the current design, this is mostly for registration info.
        self.self_rpc_listen_host = rpc_listen_host
        self.self_rpc_listen_port = rpc_listen_port
        
        self.proxy_rpc_addr = (proxy_rpc_host, proxy_rpc_port)
        
        self.sim_delay_range_ms = sim_delay_range_ms
        self.performance_factor = performance_factor

        self.active_requests = 0
        self.processed_requests_count = 0
        self.total_processing_time_ms = 0.0
        self.request_queue = asyncio.Queue() # For incoming WORKER_REQUESTs

        self._stop_event = asyncio.Event()
        # Tasks for internal operations
        self.metrics_task: Optional[asyncio.Task] = None
        self.processor_task: Optional[asyncio.Task] = None
        
        # Connection to Proxy (Worker is client to Proxy's internal RPC)
        self.proxy_writer: Optional[asyncio.StreamWriter] = None
        self.proxy_reader: Optional[asyncio.StreamReader] = None
        self.proxy_connection_handler_task: Optional[asyncio.Task] = None


        self.logger = logging.getLogger(f"Worker-{self.worker_id}")

    async def _handle_messages_from_proxy(self):
        """Handles WORKER_REQUEST messages coming from the proxy over the persistent connection."""
        if not self.proxy_reader:
            self.logger.error("Proxy reader not available for handling messages.")
            return
        try:
            while not self._stop_event.is_set():
                message = await read_message(self.proxy_reader)
                if message is None:
                    self.logger.info("Connection to proxy closed (or read error).")
                    await self._close_proxy_connection() # Signal connection lost
                    break # Exit message handling loop
                
                msg_type = message.get("type")
                payload = message.get("payload", {})

                if msg_type == MessageType.WORKER_REQUEST.value:
                    # Put on internal queue for processing by self.processor_task
                    # Pass the payload and indicate that response should go back via self.proxy_writer
                    await self.request_queue.put((payload, "proxy_connection"))
                elif msg_type == MessageType.REGISTER_ACK.value: # Proxy ACKs our registration
                    if payload.get("status") == "ok":
                        self.logger.info(f"Successfully registered with proxy. ACK: {payload}")
                    else:
                        self.logger.error(f"Proxy registration denied: {payload}. Shutting down connection.")
                        await self._close_proxy_connection() # Close on failed registration
                        self._stop_event.set() # Stop the worker
                        break
                else:
                    self.logger.warning(f"Received unexpected message type from proxy: {msg_type}")

        except asyncio.CancelledError:
            self.logger.info("Proxy message handler task cancelled.")
        except (ConnectionResetError, BrokenPipeError):
            self.logger.warning("Connection to proxy reset/broken while reading messages.")
            await self._close_proxy_connection()
        except Exception as e:
            self.logger.error(f"Error in proxy message handler: {e}", exc_info=True)
            await self._close_proxy_connection() # Close connection on unexpected error
        finally:
            if not self._stop_event.is_set() and not (self.proxy_writer and self.proxy_writer.is_closing()):
                 self.logger.warning("Proxy message handler exited unexpectedly. Worker might lose connection.")
                 # Consider triggering a reconnect sequence here if desired.

    async def _connect_and_register_with_proxy(self) -> bool:
        if self.proxy_writer and not self.proxy_writer.is_closing():
            self.logger.info("Already connected to proxy.")
            return True
        try:
            self.logger.info(f"Attempting to connect to proxy at {self.proxy_rpc_addr}")
            reader, writer = await asyncio.open_connection(*self.proxy_rpc_addr)
            self.proxy_reader = reader
            self.proxy_writer = writer
            
            # Start task to handle incoming messages from proxy (like WORKER_REQUEST)
            self.proxy_connection_handler_task = asyncio.create_task(
                self._handle_messages_from_proxy(), 
                name=f"{self.worker_id}-ProxyMsgHandler"
            )
            
            register_payload = {
                "worker_id": self.worker_id,
                "rpc_host": self.self_rpc_listen_host, # Info about worker's own (conceptual) listening addr
                "rpc_port": self.self_rpc_listen_port
            }
            await send_message(writer, MessageType.REGISTER_WORKER, register_payload)
            self.logger.info(f"Sent REGISTER_WORKER to proxy.")
            # ACK is handled by _handle_messages_from_proxy
            return True # Connection initiated, registration sent

        except ConnectionRefusedError:
            self.logger.error(f"Proxy connection refused at {self.proxy_rpc_addr}. Proxy might not be running.")
            return False
        except Exception as e:
            self.logger.error(f"Error connecting/registering with proxy: {e}", exc_info=True)
            await self._close_proxy_connection()
            return False

    async def _close_proxy_connection(self):
        if self.proxy_connection_handler_task and not self.proxy_connection_handler_task.done():
            self.proxy_connection_handler_task.cancel()
            try:
                await self.proxy_connection_handler_task
            except asyncio.CancelledError:
                pass
        self.proxy_connection_handler_task = None

        if self.proxy_writer:
            if not self.proxy_writer.is_closing():
                self.proxy_writer.close()
            try:
                await self.proxy_writer.wait_closed()
            except Exception as e:
                self.logger.warning(f"Error during proxy writer close: {e}")
        self.proxy_writer = None
        self.proxy_reader = None
        self.logger.info("Proxy connection closed.")


    async def _send_metrics_periodically(self):
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(WORKER_METRICS_INTERVAL_SECONDS)
                
                if not self.proxy_writer or self.proxy_writer.is_closing():
                    self.logger.warning("Proxy connection for metrics lost. Attempting to reconnect...")
                    if not await self._connect_and_register_with_proxy():
                        self.logger.error("Failed to re-establish proxy connection. Metrics not sent.")
                        continue 
                
                avg_latency = (self.total_processing_time_ms / self.processed_requests_count) \
                    if self.processed_requests_count > 0 else 0.0
                
                metrics_payload = {
                    "worker_id": self.worker_id,
                    "latency_ms": avg_latency,
                    "queue_depth": self.request_queue.qsize(),
                    "active_requests": self.active_requests,
                    "processed_count": self.processed_requests_count
                }
                if self.proxy_writer and not self.proxy_writer.is_closing():
                    await send_message(self.proxy_writer, MessageType.METRICS_UPDATE, metrics_payload)
                    self.logger.debug(f"Sent METRICS_UPDATE: {metrics_payload}")
                else:
                    self.logger.warning("Cannot send metrics, proxy writer not available or closing.")

            except (ConnectionResetError, BrokenPipeError):
                self.logger.warning("Proxy connection lost while sending metrics. Will try to reconnect.")
                await self._close_proxy_connection()
            except asyncio.CancelledError:
                self.logger.info("Metrics sending task cancelled.")
                break
            except Exception as e:
                self.logger.error(f"Error in metrics sending loop: {e}", exc_info=True)
                await asyncio.sleep(WORKER_METRICS_INTERVAL_SECONDS)


    async def _process_request_from_queue(self):
        while not self._stop_event.is_set():
            try:
                # Item is (payload, response_target_info)
                # response_target_info could be "proxy_connection" or a specific writer if worker had its own server
                request_payload, response_target = await self.request_queue.get()
                
                self.active_requests += 1
                start_time = time.perf_counter()
                
                base_delay_ms = random.uniform(self.sim_delay_range_ms[0], self.sim_delay_range_ms[1])
                actual_delay_s = (base_delay_ms * self.performance_factor) / 1000.0
                await asyncio.sleep(actual_delay_s)
                
                end_time = time.perf_counter()
                processing_time_ms = (end_time - start_time) * 1000

                self.total_processing_time_ms += processing_time_ms
                self.processed_requests_count += 1
                
                response_payload = {
                    "data": f"Response from {self.worker_id} to request {request_payload.get('request_id', 'N/A')}",
                    "original_request_id": request_payload.get("request_id"),
                    "worker_id": self.worker_id,
                    "worker_processing_time_ms": processing_time_ms
                }
                
                try:
                    if response_target == "proxy_connection":
                        if self.proxy_writer and not self.proxy_writer.is_closing():
                            await send_message(self.proxy_writer, MessageType.WORKER_RESPONSE, response_payload)
                        else:
                            self.logger.warning(f"Proxy writer unavailable for WORKER_RESPONSE for {request_payload.get('request_id')}")
                    # else: # If direct connections to worker were supported
                    #     response_writer = response_target
                    #     await send_message(response_writer, MessageType.WORKER_RESPONSE, response_payload)
                except (ConnectionResetError, BrokenPipeError):
                    self.logger.warning(f"Connection to proxy lost sending WORKER_RESPONSE for {request_payload.get('request_id')}")
                    await self._close_proxy_connection() # This might be too aggressive here
                
                self.request_queue.task_done()
                self.active_requests -= 1
                self.logger.debug(f"Processed request {request_payload.get('request_id')}, "
                                 f"delay: {processing_time_ms:.2f}ms, queue: {self.request_queue.qsize()}")

            except asyncio.CancelledError:
                self.logger.info("Request processing task cancelled.")
                while not self.request_queue.empty():
                    request_payload, response_target = self.request_queue.get_nowait()
                    if response_target == "proxy_connection" and self.proxy_writer and not self.proxy_writer.is_closing():
                        error_payload = {"error": "Worker shutting down", "original_request_id": request_payload.get("request_id")}
                        asyncio.create_task(send_message(self.proxy_writer, MessageType.ERROR, error_payload))
                    self.request_queue.task_done()
                break
            except Exception as e:
                self.logger.error(f"Error in request processing loop: {e}", exc_info=True)
                if 'request_payload' in locals():
                    self.active_requests = max(0, self.active_requests - 1)
                    self.request_queue.task_done()


    async def start(self):
        self.logger.info(f"Starting up...")
        self._stop_event.clear()

        if not await self._connect_and_register_with_proxy():
            self.logger.error("Failed initial connection/registration with proxy. Worker will not start full operations.")
            # Depending on retry logic, worker might still attempt to run metrics/processor tasks that try to reconnect
            # For now, if initial connect fails, we still start tasks that might retry connection.
            # Or, one could choose to exit here. Let's let them start and retry.
        
        self.metrics_task = asyncio.create_task(self._send_metrics_periodically(), name=f"{self.worker_id}-Metrics")
        self.processor_task = asyncio.create_task(self._process_request_from_queue(), name=f"{self.worker_id}-Processor")

        self.logger.info(f"Worker {self.worker_id} core tasks started.")

    async def stop(self):
        self.logger.info(f"Stopping...")
        self._stop_event.set()

        tasks_to_cancel = [self.metrics_task, self.processor_task, self.proxy_connection_handler_task]
        for task in tasks_to_cancel:
            if task and not task.done():
                task.cancel()
        
        # Wait for tasks with timeout
        if self.metrics_task:
            try: await asyncio.wait_for(self.metrics_task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError): pass
        if self.processor_task:
            try: await asyncio.wait_for(self.processor_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError): pass
        if self.proxy_connection_handler_task: # This might already be cancelled by _close_proxy_connection
            try: await asyncio.wait_for(self.proxy_connection_handler_task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError): pass
        
        await self._close_proxy_connection() # Ensure final close

        self.logger.info(f"Stopped. Processed {self.processed_requests_count} requests.")