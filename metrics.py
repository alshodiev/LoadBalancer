from typing import NamedTuple, Optional, Dict, Any

class BenchmarkResult(NamedTuple):
    request_id: str
    send_time: float
    response_time: float
    latency_ms: float
    success: bool
    status_code: Optional[str] 
    response_payload: Optional[Dict[str, Any]]

# Could add functions here for formatting or saving metrics if they become complex
# e.g., def save_metrics_to_csv(filename, data, fieldnames): ...
# For now, proxy and benchmark_driver handle their own CSV outputs.