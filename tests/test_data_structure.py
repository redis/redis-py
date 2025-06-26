import concurrent
import random
from concurrent.futures import ThreadPoolExecutor
from time import sleep

from redis.data_structure import WeightedList


class TestWeightedList:
    def test_add_items(self):
        wlist = WeightedList()

        wlist.add('item1', 3.0)
        wlist.add('item2', 2.0)
        wlist.add('item3', 4.0)
        wlist.add('item4', 4.0)

        assert wlist.get_top_n(4) == [('item3', 4.0), ('item4', 4.0), ('item1', 3.0), ('item2', 2.0)]

    def test_remove_items(self):
        wlist = WeightedList()
        wlist.add('item1', 3.0)
        wlist.add('item2', 2.0)
        wlist.add('item3', 4.0)
        wlist.add('item4', 4.0)

        assert wlist.remove('item2') == 2.0
        assert wlist.remove('item4') == 4.0

        assert wlist.get_top_n(4) == [('item3', 4.0), ('item1', 3.0)]

    def test_get_by_weight_range(self):
        wlist = WeightedList()
        wlist.add('item1', 3.0)
        wlist.add('item2', 2.0)
        wlist.add('item3', 4.0)
        wlist.add('item4', 4.0)

        assert wlist.get_by_weight_range(2.0, 3.0) == [('item1', 3.0), ('item2', 2.0)]

    def test_update_weights(self):
        wlist = WeightedList()
        wlist.add('item1', 3.0)
        wlist.add('item2', 2.0)
        wlist.add('item3', 4.0)
        wlist.add('item4', 4.0)

        assert wlist.get_top_n(4) == [('item3', 4.0), ('item4', 4.0), ('item1', 3.0), ('item2', 2.0)]

        wlist.update_weight('item2', 5.0)

        assert wlist.get_top_n(4) == [('item2', 5.0), ('item3', 4.0), ('item4', 4.0), ('item1', 3.0)]

    def test_thread_safety(self) -> None:
        """Test thread safety with concurrent operations"""
        wl = WeightedList()

        def worker(worker_id):
            for i in range(100):
                # Add items
                wl.add(f"item_{worker_id}_{i}", random.uniform(0, 100))

                # Read operations
                try:
                    length = len(wl)
                    if length > 0:
                        top_items = wl.get_top_n(min(5, length))
                        items_in_range = wl.get_by_weight_range(20, 80)
                except Exception as e:
                    print(f"Error in worker {worker_id}: {e}")

                sleep(0.001)  # Small delay

        # Run multiple workers concurrently
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(worker, i) for i in range(5)]
            concurrent.futures.wait(futures)

        assert len(wl) == 500