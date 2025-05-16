import asyncio
import json
import logging
import time
import uuid
import csv
import os
from typing import List, Optional, Dict, Any

from rpc import MessageType, send_message, read_message
from metrics import BenchmarkResult 
from config import (
    BENCHMARK_TIMEOUT_SECONDS, RESULTS_DIR # General benchmark config
)

logger = logging.getLogger(__name__)

async def run_single_client_session(client_id: int, proxy_host: str, proxy_port: int, 
                                    num_requests: int, request_interval_s: float) -> List[BenchmarkResult]:
    results: List[BenchmarkResult] = []
    try:
        reader, writer = await asyncio.open_connection(proxy_host, proxy_port)
    except Exception as e:
        logger.error(f"Client-{client_id}: Failed to connect to proxy {proxy_host}:{proxy_port}: {e}")
        now = time.perf_counter()
        for i in range(num_requests):
            req_id = f"client{client_id}-req{i}-{uuid.uuid4().hex[:8]}"
            results.append(BenchmarkResult(req_id, now, now, 0, False, "CONNECTION_ERROR", None))
        return results

    logger.info(f"Client-{client_id}: Connected to proxy.")
    
    for i in range(num_requests):
        request_id = f"client{client_id}-req{i}-{uuid.uuid4().hex[:8]}"
        request_payload = {"data": f"Benchmark hello from client {client_id}, req {i}", "request_id": request_id}
        
        send_time = time.perf_counter()
        latency_ms = 0.0
        success = False
        status_code = "UNKNOWN_ERROR"
        response_data: Optional[Dict[str, Any]] = None

        try:
            await send_message(writer, MessageType.CLIENT_REQUEST, request_payload)
            response_msg = await asyncio.wait_for(read_message(reader), timeout=BENCHMARK_TIMEOUT_SECONDS)
            recv_time = time.perf_counter()
            latency_ms = (recv_time - send_time) * 1000

            if response_msg and response_msg.get("type") == MessageType.CLIENT_RESPONSE.value:
                response_data = response_msg.get("payload")
                if response_data and "error" in response_data:
                    success = False
                    status_code = str(response_data.get("error", "WORKER_OR_PROXY_ERROR"))[:50]
                else:
                    success = True
                    status_code = "SUCCESS"
            elif response_msg is None:
                success = False; status_code = "PROXY_DISCONNECTED"
                logger.warning(f"Client-{client_id} Req-{request_id}: Proxy disconnected.")
                break 
            else:
                success = False; status_code = f"UNEXPECTED_RESPONSE_TYPE:{str(response_msg.get('type'))[:20]}"
        except asyncio.TimeoutError:
            latency_ms = BENCHMARK_TIMEOUT_SECONDS * 1000
            success = False; status_code = "TIMEOUT"
        except (ConnectionResetError, BrokenPipeError) as e:
            latency_ms = (time.perf_counter() - send_time) * 1000 # approx
            success = False; status_code = "CONNECTION_ERROR"
            logger.error(f"Client-{client_id} Req-{request_id}: Connection error: {e}. Aborting session.")
            break
        except Exception as e:
            latency_ms = (time.perf_counter() - send_time) * 1000 # approx
            success = False; status_code = f"CLIENT_EXCEPTION:{type(e).__name__}"
            logger.error(f"Client-{client_id} Req-{request_id}: Exception: {e}", exc_info=True)
        
        results.append(BenchmarkResult(request_id, send_time, time.perf_counter(), latency_ms, success, status_code, response_data))
        
        if i < num_requests - 1:
            await asyncio.sleep(request_interval_s)

    if writer and not writer.is_closing():
        writer.close()
        try:
            await writer.wait_closed()
        except Exception: pass
    logger.info(f"Client-{client_id}: Session finished. Logged {len(results)} results.")
    return results

async def run_benchmark(proxy_host: str, proxy_port: int, 
                        num_clients: int, requests_per_client: int, 
                        request_interval_ms: int, strategy_name: str):
    logger.info(f"Starting benchmark: {num_clients} clients, {requests_per_client} reqs/client, "
                f"{request_interval_ms}ms interval. Proxy: {proxy_host}:{proxy_port}, Strategy: {strategy_name}")
    
    request_interval_s = request_interval_ms / 1000.0
    start_time = time.perf_counter()
    
    client_tasks = [
        run_single_client_session(i, proxy_host, proxy_port, requests_per_client, request_interval_s)
        for i in range(num_clients)
    ]
    all_results_nested = await asyncio.gather(*client_tasks, return_exceptions=True)
    total_benchmark_time_s = time.perf_counter() - start_time

    all_results: List[BenchmarkResult] = []
    for res_list in all_results_nested:
        if isinstance(res_list, Exception):
            logger.error(f"Benchmark client session failed entirely: {res_list}")
        elif isinstance(res_list, List):
            all_results.extend(res_list)

    if not os.path.exists(RESULTS_DIR):
        os.makedirs(RESULTS_DIR)

    total_requests_attempted = num_clients * requests_per_client
    successful_requests = sum(1 for r in all_results if r.success)
    
    logger.info(f"Benchmark finished in {total_benchmark_time_s:.2f}s.")
    logger.info(f"Total requests attempted: {total_requests_attempted}")
    logger.info(f"Total results logged: {len(all_results)}")
    logger.info(f"Successful requests: {successful_requests}")

    if not all_results:
        logger.warning("No results collected from benchmark.")
        return

    latencies_ms = [r.latency_ms for r in all_results if r.success] # Only successful for latency stats
    avg_latency_ms = sum(latencies_ms) / len(latencies_ms) if latencies_ms else 0
    throughput_rps = successful_requests / total_benchmark_time_s if total_benchmark_time_s > 0 else 0
    success_rate = (successful_requests / len(all_results)) * 100 if all_results else 0

    percentiles = {}
    if latencies_ms:
        sorted_latencies = sorted(latencies_ms)
        for p_val in [50, 90, 95, 99]:
            idx = min(int(len(sorted_latencies) * (p_val / 100.0)), len(sorted_latencies) - 1)
            percentiles[f"p{p_val}_latency_ms"] = sorted_latencies[idx] if sorted_latencies else 0
    else:
        for p_val in [50, 90, 95, 99]: percentiles[f"p{p_val}_latency_ms"] = 0

    logger.info(f"Success Rate: {success_rate:.2f}%")
    logger.info(f"Throughput: {throughput_rps:.2f} RPS")
    logger.info(f"Avg Latency (ok): {avg_latency_ms:.2f} ms. " + ", ".join([f"p{k[1:]}={v:.2f}" for k,v in percentiles.items()]))

    csv_filename = os.path.join(RESULTS_DIR, f"benchmark_results_{strategy_name}_{time.strftime('%Y%m%d-%H%M%S')}.csv")
    with open(csv_filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(BenchmarkResult._fields) # Use NamedTuple fields for header
        for r in all_results:
            writer.writerow(list(r)) # Convert NamedTuple to list for writerow
    logger.info(f"Detailed benchmark results saved to {csv_filename}")

    summary_filename = os.path.join(RESULTS_DIR, "benchmark_summary.csv")
    summary_fields = ["timestamp", "strategy", "num_clients", "requests_per_client", 
                      "request_interval_ms", "total_benchmark_time_s", "total_requests_attempted", 
                      "total_results_logged", "successful_requests", "success_rate_percent", 
                      "throughput_rps", "avg_latency_ms"] + list(percentiles.keys())
    summary_data = {
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'), "strategy": strategy_name,
        "num_clients": num_clients, "requests_per_client": requests_per_client,
        "request_interval_ms": request_interval_ms, "total_benchmark_time_s": round(total_benchmark_time_s, 2),
        "total_requests_attempted": total_requests_attempted, "total_results_logged": len(all_results),
        "successful_requests": successful_requests, "success_rate_percent": round(success_rate, 2),
        "throughput_rps": round(throughput_rps, 2), "avg_latency_ms": round(avg_latency_ms, 2),
        **{k: round(v, 2) for k,v in percentiles.items()}
    }
    file_exists = os.path.isfile(summary_filename)
    with open(summary_filename, 'a', newline='') as f:
        csv_writer = csv.DictWriter(f, fieldnames=summary_fields)
        if not file_exists: csv_writer.writeheader()
        csv_writer.writerow(summary_data)
    logger.info(f"Benchmark summary appended to {summary_filename}")