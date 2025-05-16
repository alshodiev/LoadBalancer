import asyncio
import logging
from collections import deque
from abc import ABC, abstractmethod
from typing import NamedTuple, Dict, Any, Tuple, Optional, Deque

from config import EWMA_ALPHA

logger = logging.getLogger(__name__)

class WorkerInfo(NamedTuple):
    worker_id: str
    # rpc_addr is the address the worker *listens* on for requests from the proxy.
    # However, in current design, worker connects to proxy, and that connection is used.
    # So rpc_addr might be more for info, and `writer` is the key communication channel.
    rpc_addr: Tuple[str, int] # (host, port) where worker *would* listen if proxy connected to it.
    # Metrics
    latency_ms: float = float('inf')
    queue_depth: int = 0
    active_requests: int = 0
    ewma_latency_ms: float = float('inf')
    # writer is the stream to send requests *to* this worker.
    # This is established when worker connects to proxy's internal RPC.
    writer: Optional[asyncio.StreamWriter] = None


class LoadBalancingStrategy(ABC):
    def __init__(self):
        self.workers: Dict[str, WorkerInfo] = {}
        self.worker_ids_list: Deque[str] = deque()

    @abstractmethod
    def select_worker(self, request_payload: Dict[str, Any]) -> Optional[str]:
        pass

    def add_worker(self, worker_id: str, worker_rpc_addr_info: Tuple[str, int], writer: asyncio.StreamWriter):
        if worker_id not in self.workers:
            self.workers[worker_id] = WorkerInfo(
                worker_id=worker_id,
                rpc_addr=worker_rpc_addr_info, # Store info about worker's declared listening addr
                writer=writer # This is the actual writer for proxy->worker comms
            )
            self.worker_ids_list.append(worker_id)
            logger.info(f"LB: Added worker {worker_id} (connects from {writer.get_extra_info('peername')}, says listens on {worker_rpc_addr_info})")
        else:
            # Potentially update if re-registering with a new connection/writer
            self.workers[worker_id] = self.workers[worker_id]._replace(writer=writer, rpc_addr=worker_rpc_addr_info)
            logger.info(f"LB: Updated writer/info for existing worker {worker_id}")


    def remove_worker(self, worker_id: str):
        if worker_id in self.workers:
            removed_worker_info = self.workers.pop(worker_id)
            if removed_worker_info.writer and not removed_worker_info.writer.is_closing():
                 # logger.debug(f"LB: Closing writer for removed worker {worker_id} during LB removal.")
                 removed_worker_info.writer.close() # Try to close it if LB is causing removal
            try:
                self.worker_ids_list.remove(worker_id)
            except ValueError:
                pass
            logger.info(f"LB: Removed worker {worker_id}")
        else:
            logger.warning(f"LB: Attempted to remove non-existent worker {worker_id}")

    def update_worker_metrics(self, worker_id: str, metrics: Dict[str, Any]):
        if worker_id in self.workers:
            current_info = self.workers[worker_id]
            new_latency = metrics.get('latency_ms', current_info.latency_ms)
            
            if current_info.ewma_latency_ms == float('inf'):
                new_ewma_latency = new_latency
            else:
                new_ewma_latency = (EWMA_ALPHA * new_latency) + \
                                   ((1 - EWMA_ALPHA) * current_info.ewma_latency_ms)

            self.workers[worker_id] = current_info._replace(
                latency_ms=new_latency,
                queue_depth=metrics.get('queue_depth', current_info.queue_depth),
                active_requests=metrics.get('active_requests', current_info.active_requests),
                ewma_latency_ms=new_ewma_latency
            )
        else:
            logger.warning(f"LB: Metrics received for unknown worker {worker_id}")
    
    def get_worker_writer(self, worker_id: str) -> Optional[asyncio.StreamWriter]:
        worker_info = self.workers.get(worker_id)
        return worker_info.writer if worker_info and worker_info.writer and not worker_info.writer.is_closing() else None

class RoundRobinStrategy(LoadBalancingStrategy):
    def __init__(self):
        super().__init__()
        self._current_index = 0

    def select_worker(self, request_payload: Dict[str, Any]) -> Optional[str]:
        if not self.worker_ids_list:
            return None
        
        num_workers = len(self.worker_ids_list)
        if num_workers == 0: return None

        # Try up to num_workers times to find a worker with a valid writer
        for _ in range(num_workers):
            self._current_index = self._current_index % num_workers
            selected_worker_id = self.worker_ids_list[self._current_index]
            self._current_index = (self._current_index + 1) % num_workers
            
            worker_info = self.workers.get(selected_worker_id)
            if worker_info and worker_info.writer and not worker_info.writer.is_closing():
                return selected_worker_id
            # else: worker missing or writer invalid, try next in round-robin sequence

        logger.warning("RoundRobin: No worker with a valid writer found after checking all.")
        return None # No valid worker found

    def remove_worker(self, worker_id: str):
        if worker_id in self.worker_ids_list:
            try:
                idx_removed = self.worker_ids_list.index(worker_id)
                super().remove_worker(worker_id) # This removes from self.workers and self.worker_ids_list
                if self.worker_ids_list: # If list is not empty after removal
                    if idx_removed < self._current_index:
                        self._current_index -= 1
                    self._current_index %= len(self.worker_ids_list) # Ensure index is valid
                else:
                    self._current_index = 0 # Reset if no workers left
            except ValueError:
                 super().remove_worker(worker_id) # Fallback if not in deque (shouldn't happen)
        else:
            super().remove_worker(worker_id)


class LeastLoadedStrategy(LoadBalancingStrategy):
    def select_worker(self, request_payload: Dict[str, Any]) -> Optional[str]:
        if not self.workers:
            return None
        
        available_workers = [
            info for info in self.workers.values() 
            if info.writer and not info.writer.is_closing()
        ]
        if not available_workers:
            return None

        best_worker = min(available_workers, key=lambda w: (w.queue_depth, w.active_requests, w.worker_id))
        return best_worker.worker_id

class EWMAStrategy(LoadBalancingStrategy):
    def select_worker(self, request_payload: Dict[str, Any]) -> Optional[str]:
        if not self.workers:
            return None

        available_workers = [
            info for info in self.workers.values() 
            if info.writer and not info.writer.is_closing()
        ]
        if not available_workers:
            return None

        best_worker = min(available_workers, key=lambda w: (w.ewma_latency_ms, w.worker_id))
        return best_worker.worker_id