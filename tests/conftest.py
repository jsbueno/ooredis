import redis


import pytest


@pytest.fixture
def r():
    # TBD: work on having a redis in a contianer for running tests
    r = redis.Redis()
    yield r
    r.close()

