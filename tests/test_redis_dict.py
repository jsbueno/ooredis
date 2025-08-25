from ooredis import RedisDict


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

