import json
import pickle
import uuid
from collections.abc import MutableMapping, Mapping
from redis import Redis
import hashlib, hmac

import typing as t


class RedisMap(MutableMapping):
    _digest_len = 32
    def __init__(
        self,
        redis_conn: Redis,
        prefix: str | bytes | bytearray = None,
        key: bytes | bytearray = b"",
        data: t.Optional[Mapping] = None,
        /,
        **kwargs,
    ):
        self.redis = redis_conn
        if not isinstance(prefix, bytes | bytearray | None):
            prefix = prefix.encode()
        self.prefix = prefix or bytes(uuid.uuid4())
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

    def _redis_key(self, key):
        if  isinstance(key, str):
            key = key.encode()
        redis_key = b":".join((self.prefix, key))
        return redis_key

    def _calc_digest(self, payload):
        return hmac.new(self.key, payload, hashlib.sha256).digest()

    def __getitem__(self, key):
        data = self.redis.get(self._redis_key(key))
        payload = data[:-self._digest_len]
        redis_digest = data[-self._digest_len:]
        if redis_digest != self._calc_digest(payload):
            raise ValueError("Data retrieved from redis incompatible with signing key for this instance. Not de-serializing")
        return pickle.loads(payload)

    def __setitem__(self, key, value):
        payload = pickle.dumps(value)
        digest = self._calc_digest(payload)
        self.redis.set(self._redis_key(key), payload + digest)

    def __delitem__(self, key):
        ...

    def __iter__(self):
        raise NotImplementedError()

    def __len__(self):
        raise NotImplementedError()


