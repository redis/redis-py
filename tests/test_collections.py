# -*- coding: utf-8 -*-
import pytest
import redis.collections as p
import collections


class TestRedisList(object):
    def test_list(self, sr):
        l = p.RedisList('foo', redis=sr)
        assert 0 == len(l)
        with pytest.raises(StopIteration):
            l.__iter__().__next__()
        l.append(1)
        assert 1 == len(l)
        assert 1 == l[0]
        l.append(3)
        assert 3 == l[1]
        assert 2 == len(l)
        assert 1 == l[0]
        l[1] = 2
        assert 2 == l[1]
        assert 2 == l[-1]
        assert 1 == l[-2]
        assert [1, 2] == list(l)
        assert 2 == len(l)
        with pytest.raises(IndexError):
            l[2]
        assert 3 == sum(l)

        l.insert(1, 'x')
        assert 'x', l[1] == str(list(l))
        assert [1, 'x', 2] == list(l)

        l.insert(3, 'z')
        assert l[3] == 'z'

        assert ['z', 2, 'x', 1] == list(l.__reversed__())

        assert 4 == len(l)
        assert 3 == l.index('z')
        with pytest.raises(ValueError):
            l.remove('t')
        assert 4 == len(l)
        assert 'z' in l
        l.remove('z')
        assert 3 == len(l)
        assert not ('z' in l)

        l.extend([7, 8, 9])
        assert [1, 'x', 2, 7, 8, 9] == list(l)
        assert (1 in l)

        del l[3]
        assert [1, 'x', 2, 8, 9] == list(l)

        assert 1 == l.index('x')
        with pytest.raises(ValueError):
            l.index('t')

        assert 9 == l.pop()
        assert [1, 'x', 2, 8] == list(l)

        assert 'x' == l.pop(1)
        assert [1, 2, 8] == list(l)
        with pytest.raises(IndexError):
            l.pop(3)

        l.clear()
        with pytest.raises(IndexError):
            l.pop(0)
        with pytest.raises(IndexError):
            l.pop(-1)

        assert 0 == len(l)
        l.append(1)
        with pytest.raises(IndexError):
            l.pop(1)

    def test_copy(self, sr):
        l = p.RedisList('l', sr)
        l.extend([1, 2, 3, 4])
        assert [1, 2, 3, 4] == l.copy()

    def test_str(self, sr):
        l = p.RedisList('l', sr)
        l.extend([1, 2, 3, 4])
        assert str([1, 2, 3, 4]) == str(l)
        l.append('x')
        assert str([1, 2, 3, 4, 'x']) == str(l)


class TestObjectRedis(object):
    def test_basic_dict(self, sr):
        d = p.ObjectRedis(sr)
        assert len(d) == 0
        with pytest.raises(KeyError):
            d[True]
        d[True] = 42.1
        assert len(d) == 1
        assert d[True] == 42.1
        d[3] = 'three'  # turns out d[1] and d[True] are equivalent in the standard implementations
        assert d[3] == 'three'
        assert len(d) == 2

        assert {True, 3} == set(d.keys())
        assert {True: 42.1, 3: 'three'} == d.copy()

    def test_namespace(self, sr):
        d = p.ObjectRedis(sr, namespace="foo")
        assert len(d) == 0
        with pytest.raises(KeyError):
            d[True]
        d[True] = 42.1
        assert len(d) == 1
        assert d[True] == 42.1
        d[1] = 'one'
        assert d[1] == 'one'
        assert len(d) == 2

        d2 = p.ObjectRedis(sr)
        assert len(d2) == 0  # This instance won't unpickle keys from the other instance, so can't see them
        d2[True] = 38
        assert len(d) == 2
        assert len(d2) == 1
        assert d2[True] == 38
        assert d[True] == 42.1

    def test_storing_collections(self, sr):
        d = p.ObjectRedis(sr)
        d['list'] = [1, 2, 3, 4, 5]
        assert [1, 2, 3, 4, 5] == d['list'].copy()
        d['map'] = {'a': 'red', 'b': 'blue'}
        assert {'a': 'red', 'b': 'blue'} == d['map'].copy()
        od = collections.OrderedDict()
        od[89.7] = 'WCPE'
        od[91.5] = 'WUNC'
        d['od'] = od
        assert od == d['od'].copy()
        s = {'oats', 'peas', 'beans'}
        d['set'] = s
        assert s == d['set'].copy()


class TestRedisDict(object):
    def test_values(self, sr):
        d = p.RedisDict('foo', redis=sr)
        d['a'] = 'b'
        assert 'b' == d['a']
        assert list({'a': 'b'}.__iter__()) == list(d.__iter__())
        assert ['b'] == list(d.values())
        with pytest.raises(TypeError):
            d[['a']] = 'b'

    def test_keys(self, sr):
        d = p.RedisDict('foo', redis=sr)
        d['a'] = 'A'
        d['c'] = 'C'
        assert {'a', 'c'} == set(d.keys())

    def test_copy(self, sr):
        d = p.RedisDict('foo', redis=sr)
        d['a'] = 'A'
        d['c'] = 'C'
        assert {'a': 'A', 'c': 'C'} == d.copy()

    def test_str(self, sr):
        d = p.RedisDict('foo', redis=sr)
        refd = {'sunshine': 'rainbows', 'moon': 'eclipse'}
        d.update(refd)
        assert "{'moon': 'eclipse', 'sunshine': 'rainbows'}" == str(d) or \
               "{'sunshine': 'rainbows', 'moon': 'eclipse'}" == str(d)


class TestRedisSortedSet(object):
    def test_sorted(self, sr):
        s = p.RedisSortedSet('foo', redis=sr)
        assert 0 == len(s)
        assert not ('bar' in s)
        s['bar'] = 5
        assert ('bar' in s)
        assert 5.0 == s['bar']
        with pytest.raises(TypeError):
            s['bar'] = 'baz'
        od = collections.OrderedDict()
        od['bar'] = 5.0
        assert od == s.copy()
        assert 1 == len(s)

        assert not ('bat' in s)
        s['bat'] = 1
        assert ('bat' in s)
        assert 1.0 == s['bat']
        od['bat'] = 1.0
        od.move_to_end('bar')
        assert od == s.copy()

    def test_iter(self, sr):
        s = p.RedisSortedSet('foo', redis=sr)
        s['tigers'] = 2
        s['lions'] = 1
        s['bears'] = 3
        i = s.__iter__()
        assert 'lions' == next(i)
        assert 'tigers' == next(i)
        assert 'bears' == next(i)

    def test_update(self, sr):
        s = p.RedisSortedSet('foo', redis=sr)
        s.update({'red': 650, 'green': 510, 'blue': 475})
        od = collections.OrderedDict()
        od['blue'] = 475
        od['green'] = 510
        od['red'] = 650
        assert od == s.copy()

    def test_hashable_key(self, sr):
        s = p.RedisSortedSet('foo', redis=sr)
        with pytest.raises(TypeError):
            s[['nohash']] = 1

    def test_str(self, sr):
        s = p.RedisSortedSet('foo', redis=sr)
        s.update({'H': 1, 'He': 2, 'Li': 3})
        assert "RedisSortedSet({'H': 1.0, 'He': 2.0, 'Li': 3.0})" == str(s)


class TestRedisSet(object):
    def test_set(self, sr):
        s = p.RedisSet('foo', redis=sr)
        s.add('grunge')
        assert ('grunge' in s)
        s.add(True)
        s.add(('graph'))
        assert {'grunge', True, ('graph')} == set(s)
        assert 3 == len(s)
        assert 3 == sum(1 for _ in s)

        with pytest.raises(TypeError):
            s.add(['nohash'])

        s.clear()
        assert 0 == len(s)

    def test_zero_element(self, sr):
        s = p.RedisSet('foo', redis=sr)
        s.add(0)
        assert 1 == len(s)
        assert {0} == s.copy()
        assert {0} == set(s)

        s.add(None)
        assert 2 == len(s)
        assert {0, None} == s.copy()
        assert {0, None} == set(s)

    def test_update(self, sr):
        s = p.RedisSet('bar', redis=sr)
        ref = {'oats', 'peas', 'beans'}
        s.update(tuple(ref))
        assert ref == set(s)
        assert ref == s.copy()

    def test_update_with_nothing(self, sr):
        s = p.RedisSet('bar', sr)
        s.update([])  # should have no effect
        assert set() == set(s)
        assert set() == s.copy()
        assert 0 == len(s)

        s.update()
        assert 0 == len(s)

        with pytest.raises(TypeError):
            s.update(None)

        with pytest.raises(TypeError):
            s.update(0)

    def test_bigger_set(self, sr):
        s = p.RedisSet('bar', sr)

        ref = set(range(0, 10000))
        s.update(ref)
        assert len(ref) == len(s)
        assert ref == s.copy()
        assert ref == set(s.__iter__())
        s.clear()
        assert 0 == len(s)

    def test_str(self, sr):
        s = p.RedisSet('bar', sr)
        s.update(['foo', 'bar'])
        assert "{'foo', 'bar'}" == str(s) or "{'bar', 'foo'}" == str(s)