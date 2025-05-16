import logging

# General
LOG_LEVEL = logging.INFO  # DEBUG for more verbosity
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s'
LOG_FILE = "load_balancer_simulation.log" # Will be created in the project root
RESULTS_DIR = "results"  # For benchmark CSV and proxy logs

# Proxy Config
PROXY_CLIENT_HOST = '127.0.0.1'
PROXY_CLIENT_PORT = 8888
PROXY_INTERNAL_RPC_HOST = '127.0.0.1' # Workers connect to this host
PROXY_INTERNAL_RPC_PORT = 8889      # Workers connect to this port

# Worker Config
WORKER_BASE_RPC_PORT = 9000  # Workers will listen on ports 9000, 9001, ...
WORKER_METRICS_INTERVAL_SECONDS = 2  # How often workers send metrics
WORKER_SIMULATED_PROCESSING_TIME_MS_MIN = 50
WORKER_SIMULATED_PROCESSING_TIME_MS_MAX = 200
# Example: worker_performance_factors = { "worker-0": 1.0, "worker-1": 0.5, "worker-2": 1.5 }
# Defined in simulation.py for specific runs, or could be loaded from a config file.

# RPC Config
RPC_BUFFER_SIZE = 4096
RPC_MESSAGE_LENGTH_BYTES = 4  # For framing: 4-byte integer prefix for message length

# EWMA Config
EWMA_ALPHA = 0.2  # Smoothing factor for EWMA

# Benchmark Config
BENCHMARK_NUM_CLIENTS = 10
BENCHMARK_REQUESTS_PER_CLIENT = 20
BENCHMARK_REQUEST_INTERVAL_MS = 50  # Interval between requests for a single client
BENCHMARK_TIMEOUT_SECONDS = 10      # Timeout for a single request from client to proxy