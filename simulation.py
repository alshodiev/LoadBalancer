import asyncio
import logging
import os
import time
import config 
from proxy import ProxyServer
from worker import Worker
from benchmark_driver import run_benchmark

# Global logger setup for the application
logger = logging.getLogger() # Get root logger

def setup_logging():
    # Create results directory if it doesn't exist, for the log file
    if not os.path.exists(config.RESULTS_DIR):
        os.makedirs(config.RESULTS_DIR)
    
    # Path for the main log file, ensure it's in the project root or a logs subdir
    # For simplicity, putting it in root.
    log_file_path = config.LOG_FILE 

    logging.basicConfig(
        level=config.LOG_LEVEL,
        format=config.LOG_FORMAT,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file_path, mode='w') 
        ]
    )
    logger.info(f"Logging setup complete. Log file: {log_file_path}")


async def main_simulation_run(num_workers_config: int, 
                              load_balancing_strategy_config: str,
                              worker_performance_factors_config: dict):
    logger.info(f"=== Starting Simulation: {num_workers_config} workers, Strategy: {load_balancing_strategy_config} ===")
    logger.info(f"Worker performance factors: {worker_performance_factors_config}")

    proxy = ProxyServer(
        client_host=config.PROXY_CLIENT_HOST, client_port=config.PROXY_CLIENT_PORT,
        internal_rpc_host=config.PROXY_INTERNAL_RPC_HOST, internal_rpc_port=config.PROXY_INTERNAL_RPC_PORT,
        strategy_name=load_balancing_strategy_config
    )
    
    workers: List[Worker] = []
    for i in range(num_workers_config):
        worker_id = f"worker-{i}"
        perf_factor = worker_performance_factors_config.get(worker_id, 1.0)
        worker = Worker(
            worker_id=worker_id,
            # These are for worker's own server if it were to run one.
            # In current model, worker connects out, proxy uses that stream.
            # So these are more informational for registration.
            rpc_listen_host=config.PROXY_INTERNAL_RPC_HOST, # Should be worker's host if proxy connects to it
            rpc_listen_port=config.WORKER_BASE_RPC_PORT + i, 
            # Proxy's internal RPC details, for worker to connect to
            proxy_rpc_host=config.PROXY_INTERNAL_RPC_HOST, 
            proxy_rpc_port=config.PROXY_INTERNAL_RPC_PORT,
            sim_delay_range_ms=(config.WORKER_SIMULATED_PROCESSING_TIME_MS_MIN, config.WORKER_SIMULATED_PROCESSING_TIME_MS_MAX),
            performance_factor=perf_factor
        )
        workers.append(worker)

    try:
        await proxy.start()
        
        worker_start_tasks = []
        for worker in workers:
            worker_start_tasks.append(asyncio.create_task(worker.start()))
            await asyncio.sleep(0.15) # Increased stagger slightly

        await asyncio.gather(*worker_start_tasks, return_exceptions=True)
        
        logger.info("Waiting for workers to register and system to stabilize...")
        # Wait time depends on metrics interval and number of workers.
        # A more robust system might have proxy emit an event or check count.
        await asyncio.sleep(max(2.0, config.WORKER_METRICS_INTERVAL_SECONDS * 1.5)) 
        
        num_registered = len(proxy.load_balancer.workers)
        logger.info(f"{num_registered}/{num_workers_config} workers are registered with the proxy.")
        if num_registered == 0 and num_workers_config > 0:
            logger.error("No workers registered! Benchmark will likely fail. Check worker logs for connection issues to proxy.")
        
        await run_benchmark(
            proxy_host=config.PROXY_CLIENT_HOST, proxy_port=config.PROXY_CLIENT_PORT,
            num_clients=config.BENCHMARK_NUM_CLIENTS,
            requests_per_client=config.BENCHMARK_REQUESTS_PER_CLIENT,
            request_interval_ms=config.BENCHMARK_REQUEST_INTERVAL_MS,
            strategy_name=load_balancing_strategy_config
        )

    except Exception as e:
        logger.error(f"Critical error during simulation: {e}", exc_info=True)
    finally:
        logger.info("Simulation run ended. Cleaning up...")
        
        worker_stop_tasks = [asyncio.create_task(w.stop()) for w in workers]
        if worker_stop_tasks:
             await asyncio.gather(*worker_stop_tasks, return_exceptions=True)
        
        await proxy.stop()
        
        logger.info(f"=== Simulation for {load_balancing_strategy_config} with {num_workers_config} workers COMPLETE ===")


if __name__ == "__main__":
    setup_logging() # Call this once at the beginning

    # --- Simulation Parameters ---
    NUM_WORKERS = 3
    # Define performance characteristics for workers (optional)
    # A factor of 0.5 means worker is twice as fast, 1.5 means 50% slower.
    WORKER_PERFORMANCE_FACTORS = {
        "worker-0": 0.8,  # 20% faster
        "worker-1": 1.0,  # Normal
        "worker-2": 1.2,  # 20% slower
        # Add more if NUM_WORKERS is higher, or they'll default to 1.0
    }
    # If you want all workers to be identical, set WORKER_PERFORMANCE_FACTORS = {}

    STRATEGIES_TO_RUN = ["round_robin", "least_loaded", "ewma"]
    # STRATEGIES_TO_RUN = ["round_robin"] # For a quick test

    loop = asyncio.get_event_loop()
    try:
        for strategy in STRATEGIES_TO_RUN:
            # Create a new log file for each strategy run by re-configuring file handler
            # This is a bit hacky; cleaner would be unique log filenames per run.
            # For simplicity of example, we overwrite the main log file per strategy.
            # The proxy/benchmark CSVs will be unique due to timestamps/strategy names.
            
            # If you want separate log files per run:
            # config.LOG_FILE = f"sim_run_{strategy}_{time.strftime('%Y%m%d-%H%M%S')}.log"
            # setup_logging() # Re-setup with new file name

            loop.run_until_complete(main_simulation_run(
                num_workers_config=NUM_WORKERS,
                load_balancing_strategy_config=strategy,
                worker_performance_factors_config=WORKER_PERFORMANCE_FACTORS
            ))
            if len(STRATEGIES_TO_RUN) > 1 and strategy != STRATEGIES_TO_RUN[-1]:
                 logger.info("Pausing briefly between strategy tests...")
                 loop.run_until_complete(asyncio.sleep(5)) # Longer pause
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user (Ctrl+C). Shutting down...")
    finally:
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task(loop)]
        if tasks:
            logger.info(f"Cancelling {len(tasks)} outstanding tasks before closing loop...")
            for task in tasks:
                task.cancel()
            loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        
        loop.close()
        logger.info("Event loop closed. Main simulation script finished.")