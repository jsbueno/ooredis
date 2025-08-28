import json
import pickle
import uuid
from collections.abc import MutableMapping, Mapping, MutableSequence, Sequence
from redis import Redis
import hashlib, hmac
import threading

import typing as t

from functools import cached_property

__version__ = "0.0.3"

"""
In the early interactions, the Python operations are decomposed
to single data and translated to generic redis commands even if that
is not the obviously more efficient way to do this.

As this code evolves, shortcuts may be taken so that fewer redis accesses are needed
for each action.  (e.g.: RedisDeque.extend will issue a multi-parameter lpush instead
of just repeating a Python side 'append')
"""


class _RedisBase:
    _digest_len = 32
    key: bytes
    prefix: str
    redis: Redis

    def __init__(        self,
        redis_conn: Redis,
        name: t.Optional[str] = None,
        prefix: str | bytes | bytearray = "",
        key: bytes | bytearray = b"",
    ):
        self.redis = redis_conn
        # TBD: change to a redis-backed lock in a to-be-implemented class.
        # for building-time-purposes let's go with a regular Python lock.
        self.rlock = threading.RLock()
        self.name = name or str(uuid.uuid4())
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


    @cached_property
    def _redis_name(self):
        return (self.prefix + self.name).encode()

    def _calc_digest(self, payload):
        return hmac.new(self.key, payload, hashlib.sha256).digest()

    def _decode(self, data):
        payload = data[self._digest_len:]
        redis_digest = data[:self._digest_len]
        if redis_digest != self._calc_digest(payload):
            raise ValueError("Data retrieved from redis incompatible with signing key for this instance. Not de-serializing")
        return pickle.loads(payload)




    def _encode(self, value):
        payload = pickle.dumps(value)
        digest = self._calc_digest(payload)
        # Digest first will add some impedance to people wanting to unpickle the payload directly
        return digest + payload

    def copy(self):
        """Create a redis-side copy of the data-structure

        TBD: ideally a "_XX" fuffix with XX being a count could be used
        for a new copy name, redis-side  - prefix and key remaining the same
        """
        raise NotImplementedError()
    #


class RedisDict(_RedisBase, MutableMapping):
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
        name: t.Optional[str] = None,
        prefix: str | bytes | bytearray = "",
        key: bytes | bytearray = b"",
        data: t.Optional[Mapping] = None,
    ):
        """\
            Args:
                redis_conn: a redis-connection compatible connection - can work
                    with other software providing the redis protocol, as long as they
                    have the basic Hash commands
                name: The key name for this hash in the Redis database. If not given
                    a UUID identifier will be generated.
                prefix: an optional prefix to the name - so keys related
                    to the same application will share a common prefix. If not given,
                    an empty prefix is assumed.
                key: a byte sequence used as secret to an hmac hash which
                    signs all values stored in redis - the main purpose
                    is preventing tampered data to be run through pickle.loads
                    upon reading.
                data: an optional mapping with the initial values.
                        TBD: use hmset to initialize initial data, instead of key by key
        """
        super().__init__(redis_conn, name, prefix, key)

        if data:
            self.update(data)

    def __getitem__(self, key: str) -> t.Any:
        data = self.redis.hget(self._redis_name, key.encode())
        if data is None:
            raise KeyError(key)
        return self._decode(data)

    def __setitem__(self, key: str, value: t.Any):
        self.redis.hset(self._redis_name, key.encode(), self._encode(value))

    def __delitem__(self, key: str):
        result = self.redis.hdel(self._redis_name, key.encode())
        if not result:
            raise KeyError(key)

    def __iter__(self):
        for key in self.redis.hkeys(self._redis_name):
            yield key.decode()

    def __len__(self):
        return self.redis.hlen(self._redis_name)



class RedisDeque(_RedisBase, MutableSequence):

    def __init__(
        self,
        redis_conn: Redis,
        name: t.Optional[str] = None,
        prefix: str | bytes | bytearray = "",
        key: bytes | bytearray = b"",
        data: t.Optional[Mapping] = None,
    ):
        """\
            Args:
                redis_conn: a redis-connection compatible connection - can work
                    with other software providing the redis protocol, as long as they
                    have the basic Hash commands
                name: The key name for this hash in the Redis database. If not given
                    a UUID identifier will be generated.
                prefix: an optional prefix to the name - so keys related
                    to the same application will share a common prefix. If not given,
                    an empty prefix is assumed.
                key: a byte sequence used as secret to an hmac hash which
                    signs all values stored in redis - the main purpose
                    is preventing tampered data to be run through pickle.loads
                    upon reading.
                data: an optional mapping with the initial values.
                        TBD: use hmset to initialize initial data, instead of key by key
        """
        super().__init__(redis_conn, name, prefix, key)

        if data:
                self.extend(data)
    """
        # provided by 'mutablesquence'
    def extend(self, data):
        # TBD: change to lpush with multiple parameters
        for item in data:
            self.append(data)
    """
    def extendleft(self, data):
        for item in data:
            self.append_left(item)

    def append(self, data):
        self.redis.rpush(self._redis_name, self._encode(data))

    def append_left(self, data):
        self.redis.lpush(self._redis_name, self._encode(data))


    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    def __len__(self):
        return self.redis.llen(self._redis_name)

    def pop(self):
        item = self.redis.rpop(self._redis_name)
        if not item:
            raise IndexError("Pop from empty deque")
        return self._decode(item)

    def popleft(self):
        item = self.redis.lpop(self._redis_name)
        if not item:
            raise IndexError("Pop from empty deque")
        return self._decode(item)


    def clear(self):
        self.redis.delete(self._redis_name)


    def insert(self, index, value):
        raise NotImplementedError()

    def remove(self, value, count=1) -> int:
        rvalue = self._encode(value)
        result = self.redis.lrem(self._redis_name, count, rvalue)
        if result == 0:
            raise ValueError(f"{value} not in deque")
        return result

    """



insert(i, x)
Insert x into the deque at position i.


pop()
Remove and return an element from the right side of the deque. If no elements are present, raises an IndexError.

popleft()
Remove and return an element from the left side of the deque. If no elements are present, raises an IndexError.

remove(value)


reverse()


Added in version 3.2.

rotate(n=1)
te:

maxlen

    """


    def __getitem__(self, index: int) -> t.Any:
        if index < 0:
            index += len(self)
        data = self.redis.lindex(self._redis_name, index)
        if data is None:
            raise KeyError(key)
        return self._decode(data)


    def __setitem__(self, index: int, value: t.Any):
        if index < 0:
            index += len(self)
        self.redis.lset(self._redis_name, key, self._encode(value))

    def __delitem__(self, index: int):
        """
            Redis lists don't feature a delete by index thing.

        """
        # TBD: special case elements at head or tail.
        raise NotImplementedError()
        # index = self.index(self[index])
        # self.redis.lrem(self._redis_name, 1, index)

    def __len__(self):
        return self.redis.llen(self._redis_name)
