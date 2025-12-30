import logging
from typing import List

## setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s|%(levelname)s|%(message)s")


class DatabaseSimulator:
    def __init__(self):
        # the "permanent storage"
        self.db = {}
        # the cache of potential changes
        self._cache: List[dict[any, any]] = []

    def begin(self) -> None:
        """
        initialize a new _cache DB
        """
        self._cache.append({})

    def count(self) -> int:
        """
        count they keys in the db
        """
        return len(self.db)

    def get(self, key):
        """
        return the value stored under a given key in this order
         - check if the key is in the _cache from newest to oldest
         - check if the key is in the db
         - return None if not found
        """
        if self._cache:
            for _cache_db in reversed(self._cache):
                _cache_value = _cache_db.get(key, None)
                if _cache_value:
                    return _cache_value
        # if we make it here, the key was not found in the _cache
        # return the value from the DB or None
        return self.db.get(key, None)

    def set(self, key, value) -> None:
        """
        set a value in the latest _cache db
        """
        # if the _cache is empty (not initialized) we raise an exception
        if not self._cache:
            raise Exception("No active transaction")
        # set the value in the latest _cache
        self._cache[-1][key] = value

    def commit(self) -> None:
        """
        commit the _cache DBs to the "permanent" record
        """
        # if the _cache is empty (not initialized) we raise an exception
        if not self._cache:
            raise Exception("No active transaction")
        # commit the contents of the cache
        for cache in self._cache:
            self.db.update(cache)
        # clear out the contents of the _cache (we made all the updates)
        self._cache.clear()

    def rollback(self) -> None:
        """
        rollback (discard) the last item in the _cache
        """
        if not self._cache:
            raise Exception("No active transaction")
        # remove the last item from the _cache and discard it
        self._cache.pop()


def test_DatabaseSimulator() -> None:
    # create a DB and test the functions begin, set, and commit. Test with a mix of str and int for values
    db1 = DatabaseSimulator()
    assert db1._cache == []
    db1.begin()
    assert db1._cache == [{}]
    db1.set("a", "5")
    assert db1._cache == [{"a": "5"}]
    db1.begin()
    db1.set("b", 19)
    assert db1._cache == [{"a": "5"}, {"b": 19}]
    db1.begin()
    assert db1._cache == [{"a": "5"}, {"b": 19}, {}]
    db1.commit()
    assert db1.db == {"a": "5", "b": 19}
    assert db1._cache == []
    db1.begin()
    assert db1.db == {"a": "5", "b": 19}
    assert db1._cache == [{}]

    # verify the set function only changes the latest in the _cache
    db1._cache = [{"c": 5, "e": 1}, {"d": 6}, {"e": 7}]
    db1.set("e", 99)
    assert db1._cache == [{"c": 5, "e": 1}, {"d": 6}, {"e": 99}]

    # test the count function
    assert db1.count() == 2

    # test the commit function and it clears the _cache
    db1.commit()
    assert db1.db == {"a": "5", "b": 19, "c": 5, "d": 6, "e": 99}
    assert db1._cache == []

    # tests for get function
    db1.begin()
    assert (
        db1.get("a") == "5"
    )  # check that a is "5". This is whats in the db (_cache is empty at this point)
    db1.set("a", 100)
    assert db1.get("a") == 100
    db1.begin()
    db1.set("a", 101)
    assert db1._cache == [{"a": 100}, {"a": 101}]
    assert db1.get("a") == 101
    db1.commit()
    assert db1._cache == []
    assert db1.get("a") == 101
    assert db1.db == {"a": 101, "b": 19, "c": 5, "d": 6, "e": 99}

    # tests for rollback
    # here we put some stuff into the _cache and then check the rollback function
    db1.begin()
    db1.set("d", "yep")
    db1.begin()
    db1.set("d", "nope")
    assert db1._cache == [{"d": "yep"}, {"d": "nope"}]
    db1.rollback()
    assert db1._cache == [{"d": "yep"}]


def perf_profile(func):
    """
    A decorator to show performance information about a function
    """
    import functools
    import time
    import tracemalloc

    logger = logging.getLogger(func.__name__)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        # start the memory profiler
        tracemalloc.start()
        # get the result of the function
        result = func(*args, **kwargs)
        # get a snapshot of the memory usage
        current, peak = tracemalloc.get_traced_memory()
        # stop the memory profiler
        tracemalloc.stop()
        execution_time = time.time() - start_time
        # log completed
        logger.info(
            f"{func.__name__} | time: {execution_time} | Current memory usage: {current / 10**6:.5f} MB | Peak memory usage: {peak / 10**6:.5f} MB"
        )
        return result

    return wrapper


class DatabaseSimulatorPerf(DatabaseSimulator):
    @perf_profile
    def commit(self):
        return super().commit()

    @perf_profile
    def set(self, key, value) -> None:
        return super().set(key=key, value=value)


def demo_DatabaseSimulatorPerf() -> None:
    db2 = DatabaseSimulatorPerf()
    db2.begin()
    db2.set("z", 9999)
    db2.commit()
    assert db2.db == {"z": 9999}
    assert db2._cache == []


if __name__ == "__main__":
    test_DatabaseSimulator()
    demo_DatabaseSimulatorPerf()
