import json
import pickle
import uuid
from collections.abc import MutableMapping, Mapping
from redis import Redis
import hashlib, hmac

import typing as t

from functools import cached_property

class RedisDict(MutableMapping):
    """Creates or connects to an existing Redis hashmap -
    any changes in key/values change the underlying redis data structure.

    Allows storing arbirary Python data in Redis, which can be readly used
    in other processes connected to the same redis instance. (Data is
    serialized under the hood)

    To connect from multiple process to the same hashmap, instantiate
    this with the same name, prefix and key -

    When creating the first instance, if not given,  an uuid4 is used as
    the key - it can be read from the instance.key attribute.


    The key is used to sign values into the redis instance, so that
    other instances of RedisDict will not call unpickle on unsigned data -
    data is not cryptographed.

    If a prefix is not given, it is not used - the option is there allowing
    for a soft-sharding of the redis database namespace.

    """
    _digest_len = 32
    key: bytes
    prefix: str
    redis: Redis

    def __init__(
        self,
        redis_conn: Redis,
        name: str,
        prefix: str | bytes | bytearray = "",
        key: bytes | bytearray = b"",
        data: t.Optional[Mapping] = None,
        /,
        **kwargs,
    ):
        """\
            Args:
                redis_conn: a redis-connection compatible connection - can work
                    with other software providing the redis protocol, as long as they
                    have the basic Hash commands
                name: The key name for this hash in the Redis database
                prefix: an optional prefix to the name - so keys related
                    to the same application will share a common prefix.
                key: a byte sequence used as secret to an hmac hash which
                    signs all values stored in redis - the main purpose
                    is preventing tampered data to be run through pickle.loads
                    upon reading.
                data: an optional mapping with the initial values.
                        TBD: use hmset to initialize initial data, instead of key by key
        """
        self.redis = redis_conn
        self.name = name
        if isinstance(prefix, bytes | bytearray):
            prefix = prefix.decode()
        self.prefix = prefix or ""
        if not key:
            key = uuid.uuid4().bytes
        elif isinstance(key, uuid.UUID):
            key = key.bytes
        elif isinstance(key, str):
            key = key.encode()
        self.key = key

        if data:
            self.update(data)
        self.update(kwargs)

    @cached_property
    def _redis_name(self):
        return (self.prefix + self.name).encode()

    def _calc_digest(self, payload):
        return hmac.new(self.key, payload, hashlib.sha256).digest()

    def __getitem__(self, key: str) -> t.Any:
        data = self.redis.hget(self._redis_name, key.encode())
        if data is None:
            raise KeyError(key)
        payload = data[self._digest_len:]
        redis_digest = data[:self._digest_len]
        if redis_digest != self._calc_digest(payload):
            raise ValueError("Data retrieved from redis incompatible with signing key for this instance. Not de-serializing")
        return pickle.loads(payload)

    def __setitem__(self, key: str, value: t.Any):
        payload = pickle.dumps(value)
        digest = self._calc_digest(payload)
        # Digest first will add some impedance to people wanting to unpickle the payload directly
        self.redis.hset(self._redis_name, key.encode(), digest + payload)

    def __delitem__(self, key: str):
        result = self.redis.hdel(self._redis_name, key.encode())
        if not result:
            raise KeyError(key)

    def __iter__(self):
        for key in self.redis.hkeys(self._redis_name):
            yield key.decode()

    def __len__(self):
        return self.redis.hlen(self._redis_name)


