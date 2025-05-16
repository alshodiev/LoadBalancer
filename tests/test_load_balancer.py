import pytest
from unittest.mock import MagicMock
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from load_balancer import RoundRobinStrategy, LeastLoadedStrategy, EWMAStrategy, WorkerInfo

# Mock worker details
mock_writer1 = MagicMock()
mock_writer2 = MagicMock()
mock_writer3 = MagicMock()

worker1_info = ("worker1", ("localhost", 9001), mock_writer1)
worker2_info = ("worker2", ("localhost", 9002), mock_writer2)
worker3_info = ("worker3", ("localhost", 9003), mock_writer3)


class TestRoundRobinStrategy:
    def test_empty_select(self):
        strategy = RoundRobinStrategy()
        assert strategy.select_worker({}) is None

    def test_single_worker_select(self):
        strategy = RoundRobinStrategy()
        strategy.add_worker(*worker1_info)
        assert strategy.select_worker({}) == "worker1"
        assert strategy.select_worker({}) == "worker1" # Stays on worker1

    def test_multiple_worker_cycling(self):
        strategy = RoundRobinStrategy()
        strategy.add_worker(*worker1_info)
        strategy.add_worker(*worker2_info)
        strategy.add_worker(*worker3_info)

        assert strategy.select_worker({}) == "worker1"
        assert strategy.select_worker({}) == "worker2"
        assert strategy.select_worker({}) == "worker3"
        assert strategy.select_worker({}) == "worker1" # Cycles back

    def test_remove_worker_and_cycle(self):
        strategy = RoundRobinStrategy()
        strategy.add_worker(*worker1_info)
        strategy.add_worker(*worker2_info)
        strategy.add_worker(*worker3_info)

        strategy.select_worker({})  # worker1
        strategy.select_worker({})  # worker2
        
        strategy.remove_worker("worker2")
        # Current index might have pointed to worker2 or worker3.
        # After removing worker2, list is [worker1, worker3]. Index should adjust.
        
        # Expected: worker3, then worker1
        selected_after_remove1 = strategy.select_worker({})
        selected_after_remove2 = strategy.select_worker({})
        
        assert selected_after_remove1 in ["worker1", "worker3"]
        assert selected_after_remove2 in ["worker1", "worker3"]
        assert selected_after_remove1 != selected_after_remove2

        # Add worker2 back, should resume cycling correctly
        strategy.add_worker(*worker2_info) # Added at the end of deque: [worker1, worker3, worker2]
        # If current selection was worker1, next should be worker3
        # If current selection was worker3, next should be worker2
        # This depends on the internal index management, which can be tricky.
        # A more robust test would assert the full cycle sequence after modification.


class TestLeastLoadedStrategy:
    def test_selects_least_queued(self):
        strategy = LeastLoadedStrategy()
        strategy.add_worker(*worker1_info)
        strategy.add_worker(*worker2_info)
        
        # worker1: queue=1, active=0
        # worker2: queue=0, active=1
        strategy.update_worker_metrics("worker1", {"queue_depth": 1, "active_requests": 0})
        strategy.update_worker_metrics("worker2", {"queue_depth": 0, "active_requests": 1})
        
        assert strategy.select_worker({}) == "worker2"

    def test_tie_break_on_active_requests(self):
        strategy = LeastLoadedStrategy()
        strategy.add_worker(*worker1_info)
        strategy.add_worker(*worker2_info)

        # Both queue=0
        # worker1: active=0
        # worker2: active=1
        strategy.update_worker_metrics("worker1", {"queue_depth": 0, "active_requests": 0})
        strategy.update_worker_metrics("worker2", {"queue_depth": 0, "active_requests": 1})
        assert strategy.select_worker({}) == "worker1"

# TODO: Add tests for EWMAStrategy
# TODO: Add tests for worker registration/removal effects on strategies

# To run these tests (after installing pytest: pip install pytest):
# Navigate to the `load_balancer_project` directory
# Run: pytest