from redis.utils import dict_merge, list_keys_to_dict, merge_result


class TestDictMerge:
    def test_merges_multiple_dicts(self):
        assert dict_merge({"a": 1, "b": 2}, {"c": 3}) == {"a": 1, "b": 2, "c": 3}

    def test_later_dicts_take_precedence(self):
        assert dict_merge({"a": 1, "b": 2}, {"b": 3, "c": 4}) == {
            "a": 1,
            "b": 3,
            "c": 4,
        }

    def test_no_args_returns_empty_dict(self):
        assert dict_merge() == {}

    def test_does_not_mutate_inputs(self):
        first = {"a": 1}
        dict_merge(first, {"a": 2})
        assert first == {"a": 1}


class TestMergeResult:
    def test_flattens_and_deduplicates_across_nodes(self):
        merged = merge_result("CMD", {"node1": [1, 2, 3], "node2": [3, 4]})
        assert sorted(merged) == [1, 2, 3, 4]

    def test_empty_values(self):
        assert merge_result("CMD", {"node1": []}) == []

    def test_returns_a_list(self):
        assert isinstance(merge_result("CMD", {"node1": [1], "node2": [2]}), list)


class TestListKeysToDict:
    def test_maps_every_key_to_the_callback(self):
        def callback():
            pass

        assert list_keys_to_dict(["GET", "SET"], callback) == {
            "GET": callback,
            "SET": callback,
        }
