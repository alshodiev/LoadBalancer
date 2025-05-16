# Load Balancer

## Overview
This project is a version of proxy-worker architecture I set up for my internship at Omega Therapeutics to simulate dynamic load balancing of servers. 
The core flow is as follows: Proxy routes Client requests -> Backend worker processess using Omega's internal RPC over sockets.
This mini infra-structure resembles the one I built during the internship.

## Project Structure
```
load_balancer_project/
├── config.py
├── rpc.py
├── load_balancer.py
├── worker.py
├── proxy.py
├── metrics.py
├── benchmark_driver.py
├── simulation.py
├── tests/
│   ├── __init__.py
│   ├── test_load_balancer.py  (placeholder)
│   └── test_integration.py    (placeholder)
└── results/    
```

## Features
- Accepts incoming client requests via local TCP port.
- Chooses a worker using the active load-balancing strategy.
- Routes request to selected worker and awaits response.
- Logs decision latency and routing outcome.

## Outputs
- Routing decisions with timing data.
- Benchmark results (throughput, tail latency).
- Per-request logs and load distribution summary.





