import pytest

from ooredis import RedisDict, RedisDeque


def test_redis_dict_save_and_retrieve_key(r):
    try:
        aa = RedisDict(r)
        r_name = aa._redis_name
        aa["abc"] = 23
        assert aa["abc"] == 23
    finally:
        del aa
        # TBD: create a context manager that can remove any redis keys created in a block
        r.delete(r_name)



def test_redis_dict_save_and_delete_key(r):
    try:
        aa = RedisDict(r)
        r_name = aa._redis_name
        aa["abc"] = 23
        assert aa["abc"] == 23
        del aa["abc"]
        with pytest.raises(KeyError):
            aa["abc"]
    finally:
        del aa
        # TBD: create a context manager that can remove any redis keys created in a block
        r.delete(r_name)


def test_redis_dict_len(r):
    try:
        aa = RedisDict(r)
        r_name = aa._redis_name
        for i in range(10):
            aa[f"abc_{i:03d}"] = i
            assert len(aa) == i + 1
    finally:
        del aa
        # TBD: create a context manager that can remove any redis keys created in a block
        r.delete(r_name)


def test_redis_dict_iter(r):
    try:
        aa = RedisDict(r)
        r_name = aa._redis_name
        keys = set()
        for i in range(10):
            key = f"abc_{i:03d}"
            keys.add(key)
            aa[key] = i
        for key in aa:
            keys.remove(key)
        assert not keys, "seem keys should be empty"

    finally:
        del aa
        # TBD: create a context manager that can remove any redis keys created in a block
        r.delete(r_name)




def test_redis_deque_save_and_retrieve_item(r):
    try:
        aa = RedisDeque(r)
        r_name = aa._redis_name
        aa.append(23)
        assert aa[-1] == 23
    finally:
        del aa
        # TBD: create a context manager that can remove any redis keys created in a block
        r.delete(r_name)


def test_redis_deque_save_and_pop_item(r):
    try:
        aa = RedisDeque(r)
        r_name = aa._redis_name
        aa.append(23)
        assert aa.pop() == 23
        assert len(aa) == 0
    finally:
        del aa
        # TBD: create a context manager that can remove any redis keys created in a block
        r.delete(r_name)


def test_redis_deque_clear(r):
    try:
        aa = RedisDeque(r)
        r_name = aa._redis_name
        aa.append(23)
        aa.clear()
        assert len(aa) == 0

    finally:
        del aa
        # TBD: create a context manager that can remove any redis keys created in a block
        r.delete(r_name)


def test_redis_deque_iter(r):
    try:
        aa = RedisDeque(r)
        r_name = aa._redis_name
        for i in range(5):
            aa.append(i)

        for element, check in zip(aa, range(5)):
            assert element == check
        assert len(aa) == 5
    finally:
        del aa
        # TBD: create a context manager that can remove any redis keys created in a block
        r.delete(r_name)

def test_redis_deque_extend_left(r):   # regular extend provided by collections.abc.MutableSequence
    try:
        aa = RedisDeque(r)
        r_name = aa._redis_name
        aa.extendleft(range(5))

        for element, check in zip(aa, reversed(range(5))):
            assert element == check
        assert len(aa) == 5
    finally:
        del aa
        # TBD: create a context manager that can remove any redis keys created in a block
        r.delete(r_name)
