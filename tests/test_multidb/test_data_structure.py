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

        wlist.remove('item2')
        wlist.remove('item4')

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