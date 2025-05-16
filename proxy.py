import asyncio
import json
import logging
import time
import uuid
import csv
import os
from typing import Dict, Any, Optional, List

from rpc import MessageType, send_message, read_message
from load_balancer import (LoadBalancingStrategy, RoundRobinStrategy, 
                           LeastLoadedStrategy, EWMAStrategy, WorkerInfo)
from config import (
    RESULTS_DIR, BENCHMARK_TIMEOUT_SECONDS # Used for proxy->worker timeout
)

logger = logging.getLogger(__name__)

class ProxyServer:
    def __init__(self, client_host: str, client_port: int, 
                 internal_rpc_host: str, internal_rpc_port: int, 
                 strategy_name: str = "round_robin"):
        self.client_host = client_host
        self.client_port = client_port
        self.internal_rpc_host = internal_rpc_host
        self.internal_rpc_port = internal_rpc_port
        
        self.strategy_name = strategy_name.lower()
        if self.strategy_name == "round_robin":
            self.load_balancer: LoadBalancingStrategy = RoundRobinStrategy()
        elif self.strategy_name == "least_loaded":
            self.load_balancer = LeastLoadedStrategy()
        elif self.strategy_name == "ewma":
            self.load_balancer = EWMAStrategy()
        else:
            logger.warning(f"Unknown strategy: {strategy_name}. Defaulting to RoundRobin.")
            self.load_balancer = RoundRobinStrategy()

        self.worker_writer_to_id: Dict[asyncio.StreamWriter, str] = {} # Map internal RPC writer to worker_id
        self.pending_worker_requests: Dict[str, asyncio.Future] = {} # request_id -> Future for response

        self._stop_event = asyncio.Event()
        self.client_server_task: Optional[asyncio.Task] = None
        self.internal_rpc_server_task: Optional[asyncio.Task] = None
        self._client_server_obj = None # To store asyncio.Server object
        self._internal_rpc_server_obj = None # To store asyncio.Server object
        self.request_log: List[Dict] = []

    async def _handle_client_connection(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        client_addr = client_writer.get_extra_info('peername')
        logger.info(f"Client connection from {client_addr}")
        try:
            while not self._stop_event.is_set():
                proxy_recv_time = time.perf_counter()
                message = await read_message(client_reader)
                if message is None:
                    logger.info(f"Client {client_addr} disconnected.")
                    break

                msg_type = message.get("type")
                payload = message.get("payload", {})
                request_id = payload.get("request_id", str(uuid.uuid4()))

                log_entry = {
                    "timestamp": time.time(), "request_id": request_id,
                    "client_addr": str(client_addr), "payload_recv_brief": str(payload)[:100] # Log snippet
                }

                if msg_type == MessageType.CLIENT_REQUEST.value:
                    lb_decision_start_time = time.perf_counter()
                    selected_worker_id = self.load_balancer.select_worker(payload)
                    lb_decision_latency_ms = (time.perf_counter() - lb_decision_start_time) * 1000
                    log_entry["lb_decision_latency_ms"] = lb_decision_latency_ms

                    if not selected_worker_id:
                        logger.warning(f"No worker available for request {request_id} from {client_addr}")
                        err_payload = {"error": "No backend workers available", "request_id": request_id}
                        await send_message(client_writer, MessageType.CLIENT_RESPONSE, err_payload)
                        log_entry.update({"status": "NO_WORKER", "chosen_worker_id": None})
                        self.request_log.append(log_entry)
                        continue

                    worker_writer = self.load_balancer.get_worker_writer(selected_worker_id)
                    if not worker_writer: # Checks for writer and not closing
                        logger.error(f"Selected worker {selected_worker_id} writer invalid/closed. Removing.")
                        self._remove_worker_by_id(selected_worker_id)
                        err_payload = {"error": "Selected worker became unavailable", "request_id": request_id}
                        await send_message(client_writer, MessageType.CLIENT_RESPONSE, err_payload)
                        log_entry.update({"status": "WORKER_UNAVAILABLE_POST_SELECT", "chosen_worker_id": selected_worker_id})
                        self.request_log.append(log_entry)
                        continue
                    
                    log_entry["chosen_worker_id"] = selected_worker_id
                    logger.info(f"Routing request {request_id} from {client_addr} to {selected_worker_id}")

                    response_future = asyncio.Future()
                    self.pending_worker_requests[request_id] = response_future
                    
                    proxy_to_worker_start_time = time.perf_counter()
                    worker_request_payload = payload.copy()
                    worker_request_payload["request_id"] = request_id
                    
                    try:
                        await send_message(worker_writer, MessageType.WORKER_REQUEST, worker_request_payload)
                        
                        worker_response_payload = await asyncio.wait_for(response_future, timeout=BENCHMARK_TIMEOUT_SECONDS)
                        proxy_to_worker_rtt_ms = (time.perf_counter() - proxy_to_worker_start_time) * 1000
                        
                        await send_message(client_writer, MessageType.CLIENT_RESPONSE, worker_response_payload)
                        
                        log_entry.update({
                            "status": "SUCCESS",
                            "worker_response_payload_brief": str(worker_response_payload)[:100],
                            "proxy_to_worker_rtt_ms": proxy_to_worker_rtt_ms,
                            "worker_processing_time_ms": worker_response_payload.get("worker_processing_time_ms")
                        })

                    except asyncio.TimeoutError:
                        logger.warning(f"Timeout for worker {selected_worker_id} on req {request_id}")
                        err_payload = {"error": "Worker timeout", "request_id": request_id}
                        await send_message(client_writer, MessageType.CLIENT_RESPONSE, err_payload)
                        log_entry.update({"status": "WORKER_TIMEOUT"})
                    except (ConnectionResetError, BrokenPipeError) as e:
                        logger.warning(f"Conn to worker {selected_worker_id} failed for req {request_id}: {e}. Removing.")
                        self._remove_worker_by_id(selected_worker_id, expected_writer=worker_writer)
                        err_payload = {"error": "Worker connection failed", "request_id": request_id}
                        await send_message(client_writer, MessageType.CLIENT_RESPONSE, err_payload)
                        log_entry.update({"status": "WORKER_CONNECTION_ERROR"})
                    except Exception as e:
                        logger.error(f"Error processing req {request_id} for worker {selected_worker_id}: {e}", exc_info=True)
                        err_payload = {"error": "Internal proxy error", "request_id": request_id}
                        await send_message(client_writer, MessageType.CLIENT_RESPONSE, err_payload)
                        log_entry.update({"status": "PROXY_ERROR"})
                    finally:
                        if request_id in self.pending_worker_requests: # Ensure cleanup
                            fut = self.pending_worker_requests.pop(request_id)
                            if not fut.done(): fut.cancel() # Cancel if not resolved (e.g. error before await)
                else:
                    logger.warning(f"Client {client_addr} unknown msg type: {msg_type}")
                    err_payload = {"error": "Unknown message type", "request_id": request_id}
                    await send_message(client_writer, MessageType.CLIENT_RESPONSE, err_payload)
                    log_entry.update({"status": "UNKNOWN_CLIENT_MSG_TYPE"})
                
                log_entry["overall_rtt_ms"] = (time.perf_counter() - proxy_recv_time) * 1000
                self.request_log.append(log_entry)

        except asyncio.CancelledError:
            logger.info(f"Client handler for {client_addr} cancelled.")
        except (ConnectionResetError, BrokenPipeError):
            logger.warning(f"Client connection {client_addr} reset/broken.")
        except Exception as e:
            logger.error(f"Error handling client {client_addr}: {e}", exc_info=True)
        finally:
            if client_writer and not client_writer.is_closing():
                client_writer.close()
                await client_writer.wait_closed()
            logger.info(f"Closed client connection from {client_addr}")

    async def _handle_internal_rpc_client(self, rpc_reader: asyncio.StreamReader, rpc_writer: asyncio.StreamWriter):
        peername = rpc_writer.get_extra_info('peername')
        worker_id_for_this_connection: Optional[str] = None
        logger.info(f"Internal RPC connection from {peername} (potential worker)")

        try:
            while not self._stop_event.is_set():
                message = await read_message(rpc_reader)
                if message is None:
                    logger.info(f"Internal RPC conn from {peername} closed by remote.")
                    break
                
                msg_type = message.get("type")
                payload = message.get("payload", {})

                if msg_type == MessageType.REGISTER_WORKER.value:
                    worker_id = payload.get("worker_id")
                    worker_host = payload.get("rpc_host", peername[0]) # Worker's declared host
                    worker_port = payload.get("rpc_port")           # Worker's declared port

                    if worker_id and worker_port is not None : # port can be 0
                        worker_id_for_this_connection = worker_id
                        worker_listen_addr_info = (worker_host, worker_port) # Info about worker
                        
                        # The `rpc_writer` is the connection *from* the worker *to* the proxy.
                        # Proxy uses this to send WORKER_REQUESTs back to this worker.
                        self.load_balancer.add_worker(worker_id, worker_listen_addr_info, rpc_writer)
                        self.worker_writer_to_id[rpc_writer] = worker_id
                        
                        ack_payload = {"status": "ok", "message": f"Worker {worker_id} registered."}
                        await send_message(rpc_writer, MessageType.REGISTER_ACK, ack_payload)
                        logger.info(f"Registered worker {worker_id} from {peername} (reports listening on {worker_listen_addr_info})")
                    else:
                        logger.warning(f"Invalid REGISTER_WORKER from {peername}: {payload}")
                        await send_message(rpc_writer, MessageType.ERROR, {"error": "Invalid registration payload"})
                        break 

                elif msg_type == MessageType.METRICS_UPDATE.value:
                    worker_id = payload.get("worker_id")
                    if worker_id_for_this_connection and worker_id == worker_id_for_this_connection:
                        if self.worker_writer_to_id.get(rpc_writer) == worker_id: # Security check
                             self.load_balancer.update_worker_metrics(worker_id, payload)
                        else:
                            logger.warning(f"Metrics from {worker_id} on unrecognized writer. Ignoring.")
                    else:
                        logger.warning(f"METRICS_UPDATE from {peername} with mismatched ({worker_id}) or missing worker_id. Expected {worker_id_for_this_connection}")
                
                elif msg_type == MessageType.WORKER_RESPONSE.value:
                    original_request_id = payload.get("original_request_id")
                    if original_request_id in self.pending_worker_requests:
                        future = self.pending_worker_requests.pop(original_request_id)
                        future.set_result(payload)
                    else:
                        logger.warning(f"WORKER_RESPONSE for unknown/timed-out req_id {original_request_id} from {worker_id_for_this_connection}")
                else:
                    logger.warning(f"Internal RPC from {peername} (worker: {worker_id_for_this_connection}) unknown msg type: {msg_type}")
                    await send_message(rpc_writer, MessageType.ERROR, {"error": "Unknown message type from worker"})

        except asyncio.CancelledError:
            logger.info(f"Internal RPC handler for {peername} (worker: {worker_id_for_this_connection}) cancelled.")
        except (ConnectionResetError, BrokenPipeError):
            logger.warning(f"Internal RPC conn from {peername} (worker: {worker_id_for_this_connection}) reset/broken.")
        except Exception as e:
            logger.error(f"Error internal RPC from {peername} (worker: {worker_id_for_this_connection}): {e}", exc_info=True)
        finally:
            if worker_id_for_this_connection:
                self._remove_worker_by_id(worker_id_for_this_connection, expected_writer=rpc_writer)
            # Fallback: if writer known but worker_id was not set (e.g. failed registration)
            elif rpc_writer in self.worker_writer_to_id:
                lost_worker_id = self.worker_writer_to_id.pop(rpc_writer) # Remove from map
                self._remove_worker_by_id(lost_worker_id, expected_writer=rpc_writer)


            if rpc_writer and not rpc_writer.is_closing():
                rpc_writer.close()
                await rpc_writer.wait_closed()
            logger.info(f"Closed internal RPC conn from {peername} (worker: {worker_id_for_this_connection})")

    def _remove_worker_by_id(self, worker_id: str, expected_writer: Optional[asyncio.StreamWriter] = None):
        logger.info(f"Proxy attempting to remove worker {worker_id}.")
        
        current_info = self.load_balancer.workers.get(worker_id)
        if expected_writer and current_info and current_info.writer != expected_writer:
            logger.warning(f"Skipping removal of {worker_id}: writer mismatch. Worker might have re-registered.")
            return

        self.load_balancer.remove_worker(worker_id) # LB handles closing its stored writer
        
        # Clean up worker_writer_to_id map if this writer was indeed the one
        # This map is keyed by writer, so if expected_writer is provided, use it.
        if expected_writer and expected_writer in self.worker_writer_to_id:
            if self.worker_writer_to_id[expected_writer] == worker_id:
                del self.worker_writer_to_id[expected_writer]
                logger.info(f"Removed worker {worker_id} (writer {expected_writer}) from proxy's writer map.")
            else: # Writer known, but mapped to different worker_id (should not happen if logic is correct)
                logger.warning(f"Writer {expected_writer} in map, but for worker {self.worker_writer_to_id[expected_writer]}, not {worker_id}.")
        elif not expected_writer: # If no specific writer, find by worker_id and remove (less safe)
             writer_to_del = None
             for w, wid in self.worker_writer_to_id.items():
                 if wid == worker_id:
                     writer_to_del = w
                     break
             if writer_to_del:
                 del self.worker_writer_to_id[writer_to_del]
                 logger.info(f"Removed worker {worker_id} (writer {writer_to_del}, found by ID) from proxy's writer map.")


    async def start(self):
        logger.info(f"Starting Proxy: Client on {self.client_host}:{self.client_port}, "
                    f"Internal RPC on {self.internal_rpc_host}:{self.internal_rpc_port}, "
                    f"Strategy: {self.strategy_name}")
        self._stop_event.clear()
        self.pending_worker_requests = {}
        if not os.path.exists(RESULTS_DIR):
            os.makedirs(RESULTS_DIR)

        try:
            self._client_server_obj = await asyncio.start_server(
                self._handle_client_connection, self.client_host, self.client_port
            )
            client_addr = self._client_server_obj.sockets[0].getsockname()
            logger.info(f"Client-facing server listening on {client_addr}")
            self.client_server_task = asyncio.create_task(self._client_server_obj.serve_forever(), name="ProxyClientServer")

            self._internal_rpc_server_obj = await asyncio.start_server(
                self._handle_internal_rpc_client, self.internal_rpc_host, self.internal_rpc_port
            )
            internal_addr = self._internal_rpc_server_obj.sockets[0].getsockname()
            logger.info(f"Internal RPC server listening on {internal_addr}")
            self.internal_rpc_server_task = asyncio.create_task(self._internal_rpc_server_obj.serve_forever(), name="ProxyInternalRPCServer")

        except Exception as e:
            logger.error(f"Failed to start proxy servers: {e}", exc_info=True)
            await self.stop()
            raise

        logger.info("ProxyServer fully started.")


    async def stop(self):
        logger.info("Stopping ProxyServer...")
        self._stop_event.set()

        for req_id, fut in list(self.pending_worker_requests.items()):
            if not fut.done(): fut.cancel()
        self.pending_worker_requests.clear()

        server_tasks_to_cancel = []
        if self.client_server_task and not self.client_server_task.done():
            server_tasks_to_cancel.append(self.client_server_task)
        if self.internal_rpc_server_task and not self.internal_rpc_server_task.done():
            server_tasks_to_cancel.append(self.internal_rpc_server_task)
        
        # Close server objects first to stop accepting new connections
        if self._client_server_obj:
            self._client_server_obj.close()
            await self._client_server_obj.wait_closed()
        if self._internal_rpc_server_obj:
            self._internal_rpc_server_obj.close()
            await self._internal_rpc_server_obj.wait_closed()

        # Cancel and await the serve_forever tasks
        for task in server_tasks_to_cancel:
            task.cancel()
        if server_tasks_to_cancel:
            await asyncio.gather(*server_tasks_to_cancel, return_exceptions=True)

        # Close all connected worker writers (from proxy's perspective)
        # This is now primarily handled when _remove_worker_by_id is called during worker disconnect
        # or by LB's remove_worker method. Let's ensure any remaining are closed.
        for writer in list(self.worker_writer_to_id.keys()): # Iterate on copy of keys
            if writer and not writer.is_closing():
                worker_id = self.worker_writer_to_id.get(writer, "unknown")
                logger.info(f"Proxy shutting down: closing connection to worker {worker_id}")
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception as e:
                    logger.warning(f"Error closing writer for worker {worker_id} during proxy stop: {e}")
        self.worker_writer_to_id.clear()
        # LB internal worker list also needs cleanup if not already handled by disconnects
        for worker_id in list(self.load_balancer.workers.keys()):
            self.load_balancer.remove_worker(worker_id) # LB's remove_worker should handle its writer

        logger.info("ProxyServer stopped.")
        self._save_request_log()

    def _save_request_log(self):
        if not self.request_log: return
        log_filename = os.path.join(RESULTS_DIR, f"proxy_request_log_{self.strategy_name}_{time.strftime('%Y%m%d-%H%M%S')}.csv")
        try:
            # Determine fieldnames dynamically from all logged entries to be robust
            fieldnames_set = set()
            for entry in self.request_log:
                fieldnames_set.update(entry.keys())
            fieldnames = sorted(list(fieldnames_set)) # Sort for consistent column order

            with open(log_filename, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(self.request_log)
            logger.info(f"Proxy request log saved to {log_filename}")
        except Exception as e:
            logger.error(f"Failed to save proxy request log: {e}", exc_info=True)