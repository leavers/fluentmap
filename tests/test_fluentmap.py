import asyncio
import contextlib
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, TypeVar, cast

import pytest

from fluentmap import Arguments, amap
from fluentmap import map as fmap


T = TypeVar("T")


def map_func(
    v: T,
    *vs: T,
    wait_time: float | None = None,
    exc_type: type[Exception] | None = None,
) -> T | tuple[T, ...]:
    if exc_type is not None:
        raise exc_type("mock")
    if wait_time is not None:
        time.sleep(wait_time)
    if vs:
        return (v, *vs)
    return v


def test_simple_map():
    # should accept zero-arg iterables
    assert list(fmap(map_func, ())) == list(map(map_func, ()))
    # most common case
    assert list(fmap(map_func, (0, 1, 2))) == list(map(map_func, (0, 1, 2)))
    # should accept set
    assert list(fmap(map_func, {0, 1, 2})) == list(map(map_func, {0, 1, 2}))
    # should accept str
    assert list(fmap(map_func, "012")) == list(map(map_func, "012"))
    # should accpet range
    assert list(fmap(map_func, range(6), batch_size=0)) == list(range(6))
    # should accept iterables of iterables
    assert list(fmap(map_func, ((0, 0), (1, 1)))) == list(
        map(map_func, ((0, 0), (1, 1)))
    )
    # should have same behavior with builtin map on receiving multiple iterables of
    # different length
    assert list(fmap(map_func, (0, 1, 2), (3, 4))) == list(
        map(map_func, (0, 1, 2), (3, 4))
    )
    # should accept multiple iterables of iterables of different length
    assert list(fmap(map_func, ((0, 0), (1, 1), (2, 2)), ((3, 3), (4, 4)))) == list(
        map(
            map_func,
            ((0, 0), (1, 1), (2, 2)),
            ((3, 3), (4, 4)),
        )
    )
    # test of batch_size
    assert list(fmap(map_func, range(6), batch_size=3)) == [
        [0, 1, 2],
        [3, 4, 5],
    ]
    # test of batches that cannot be rounded
    assert list(fmap(map_func, range(7), batch_size=3)) == [
        [0, 1, 2],
        [3, 4, 5],
        [6],
    ]
    # test of batches of iterables that cannot be rounded
    assert list(fmap(map_func, [(i, i) for i in range(7)], batch_size=3)) == [
        [(0, 0), (1, 1), (2, 2)],
        [(3, 3), (4, 4), (5, 5)],
        [(6, 6)],
    ]
    # chunk_size is ignored without an executor
    assert list(fmap(map_func, range(4), chunk_size=2)) == list(range(4))
    # test of on_return
    assert list(fmap(map_func, (0, 1, 2), on_return=lambda x: cast(Any, x) + 1)) == [
        1,
        2,
        3,
    ]
    # test of combination of batch_size and on_return
    assert list(
        fmap(
            map_func,
            range(7),
            batch_size=3,
            on_return=lambda row: [x + 1 for x in row],
        )
    ) == [
        [1, 2, 3],
        [4, 5, 6],
        [7],
    ]
    # test of combination of Arguments and on_return
    assert list(
        fmap(
            map_func,
            [Arguments(i, i) for i in range(2)],
            on_return=lambda row: [x + 1 for x in row],
        )
    ) == [[1, 1], [2, 2]]

    # test num_prepare works
    assert list(fmap(map_func, (0, 1, 2), num_prepare=1)) == [0, 1, 2]
    assert list(fmap(map_func, (0, 1, 2), num_prepare=2)) == [0, 1, 2]

    track: list[int]

    def wait_for_track(value: int) -> None:
        deadline = time.monotonic() + 1.0
        while value not in track and time.monotonic() < deadline:
            time.sleep(0.01)
        assert value in track

    def yield_data(n: int):
        for i in range(n):
            track.append(i)
            yield i

    def func1(res: int):
        if res == 0:
            wait_for_track(1)
        track.append(-1)
        return res

    track = []
    assert list(fmap(func1, yield_data(2), num_prepare=2)) == [0, 1]
    assert track == [0, 1, -1, -1]

    def func2(res: int):
        if res < 2:
            wait_for_track(res + 2)
        track.append(-1)
        return res

    track = []
    assert list(fmap(func2, yield_data(4), num_prepare=2)) == [0, 1, 2, 3]
    assert track == [0, 1, 2, -1, 3, -1, -1, -1]

    def func3(res: list[int]):
        if res == [0, 1]:
            wait_for_track(7)
        elif res == [2, 3]:
            wait_for_track(8)
        track.append(-1)
        return res

    track = []
    assert list(fmap(func3, yield_data(9), batch_size=2, num_prepare=3)) == [
        [0, 1],
        [2, 3],
        [4, 5],
        [6, 7],
        [8],
    ]
    assert track == [0, 1, 2, 3, 4, 5, 6, 7, -1, 8, -1, -1, -1, -1]

    # test imap stop asap
    def process_exc(res: list[int]):
        time.sleep(0.15)
        track.append(-1)
        raise RuntimeError("mock")

    track = []
    with pytest.raises(RuntimeError):
        _ = list(fmap(process_exc, yield_data(9), batch_size=2, num_prepare=2))
    assert len(track) in (7, 8)
    assert track[:7] == [0, 1, 2, 3, 4, 5, -1]
    if len(track) == 8:
        assert track[7] == 6


def test_concurrent_map():
    with ThreadPoolExecutor(max_workers=256) as executor:
        # should accept zero-arg iterables
        assert list(fmap(map_func, (), executor=executor)) == list(map(map_func, ()))
        # most common case
        assert list(fmap(map_func, (0, 1, 2), executor=executor)) == list(
            map(map_func, (0, 1, 2))
        )
        # should accept set
        assert list(fmap(map_func, {0, 1, 2}, executor=executor)) == list(
            map(map_func, {0, 1, 2})
        )
        # should accept str
        assert list(fmap(map_func, "012", executor=executor)) == list(
            map(map_func, "012")
        )
        # should accpet range
        assert list(
            fmap(
                map_func,
                range(6),
                batch_size=0,
                executor=executor,
            )
        ) == list(range(6))
        # should accept iterables of iterables
        assert list(fmap(map_func, ((0, 0), (1, 1)), executor=executor)) == list(
            map(map_func, ((0, 0), (1, 1)))
        )
        # should have same behavior with builtin map on receiving multiple iterables of
        # different length
        assert list(fmap(map_func, (0, 1, 2), (3, 4), executor=executor)) == list(
            map(map_func, (0, 1, 2), (3, 4))
        )
        # should accept multiple iterables of iterables of different length
        assert list(
            fmap(
                map_func,
                ((0, 0), (1, 1), (2, 2)),
                ((3, 3), (4, 4)),
                executor=executor,
            )
        ) == list(
            map(
                map_func,
                ((0, 0), (1, 1), (2, 2)),
                ((3, 3), (4, 4)),
            )
        )
        # test of batch_size
        assert list(fmap(map_func, range(6), batch_size=3, executor=executor)) == [
            [0, 1, 2],
            [3, 4, 5],
        ]
        # test of batches that cannot be rounded
        assert list(fmap(map_func, range(7), batch_size=3, executor=executor)) == [
            [0, 1, 2],
            [3, 4, 5],
            [6],
        ]
        # test of batches of iterables that cannot be rounded
        assert list(
            fmap(
                map_func,
                [(i, i) for i in range(7)],
                batch_size=3,
                executor=executor,
            )
        ) == [
            [(0, 0), (1, 1), (2, 2)],
            [(3, 3), (4, 4), (5, 5)],
            [(6, 6)],
        ]
        # test of chunk_size
        assert list(fmap(map_func, range(6), chunk_size=2, executor=executor)) == list(
            range(6)
        )
        # test of combination of batch_size and chunk_size
        assert list(
            fmap(
                map_func,
                range(7),
                batch_size=3,
                chunk_size=2,
                executor=executor,
            )
        ) == [
            [0, 1, 2],
            [3, 4, 5],
            [6],
        ]
        # test of on_return
        assert list(
            fmap(
                map_func,
                (0, 1, 2),
                executor=executor,
                on_return=lambda x: cast(Any, x) + 1,
            )
        ) == [1, 2, 3]
        # test of combination of batch_size and on_return
        assert list(
            fmap(
                map_func,
                range(7),
                batch_size=3,
                executor=executor,
                on_return=lambda row: [x + 1 for x in row],
            )
        ) == [
            [1, 2, 3],
            [4, 5, 6],
            [7],
        ]
        # test of combination of Arguments
        assert list(
            fmap(
                map_func,
                [Arguments(i, wait_time=0.4 - 0.2 * i) for i in range(3)],
                executor=executor,
            )
        ) == [0, 1, 2]
        # result should be inversed when sort_by_completion=True
        assert list(
            fmap(
                map_func,
                [Arguments(i, wait_time=0.4 - 0.2 * i) for i in range(3)],
                executor=executor,
                sort_by_completion=True,
            )
        ) == [2, 1, 0]
        # test of combination of sort_by_completion=True and on_return
        assert list(
            fmap(
                map_func,
                [Arguments(i, wait_time=0.4 - 0.2 * i) for i in range(3)],
                executor=executor,
                sort_by_completion=True,
                on_return=lambda x: cast(Any, x) + 1,
            )
        ) == [3, 2, 1]
        # test integrity of high-concurrency situation
        assert list(
            fmap(
                map_func,
                [Arguments(i, wait_time=random.random()) for i in range(1024)],
                executor=executor,
                on_return=lambda x: cast(Any, x) + 1,
            )
        ) == list(range(1, 1025))


def test_strict():
    # equal lengths behave as usual
    assert list(fmap(map_func, (0, 1), (2, 3), strict=True)) == [(0, 2), (1, 3)]
    # strict has no effect with a single iterable
    assert list(fmap(map_func, (0, 1), strict=True)) == [0, 1]
    # strict=False keeps the shortest-iterable behavior
    assert list(fmap(map_func, (0, 1), (2,), strict=False)) == [(0, 2)]

    def check_mismatch(**kwargs):
        with pytest.raises(ValueError) as excinfo:
            list(fmap(map_func, (0, 1), (2,), strict=True, **kwargs))
        assert str(excinfo.value) == "map() argument 2 is shorter than argument 1"
        if sys.version_info >= (3, 14):
            with pytest.raises(ValueError) as builtin_excinfo:
                list(map(map_func, (0, 1), (2,), strict=True))
            assert str(excinfo.value) == str(builtin_excinfo.value)

    check_mismatch()
    check_mismatch(num_prepare=1)
    check_mismatch(batch_size=2)
    with ThreadPoolExecutor() as executor:
        check_mismatch(executor=executor)
        check_mismatch(executor=executor, chunk_size=2)


async def aiter_range(n: int):
    for i in range(n):
        yield i


async def amap_func(
    v: T,
    *vs: T,
    wait_time: float | None = None,
    exc_type: type[Exception] | None = None,
) -> T | tuple[T, ...]:
    if exc_type is not None:
        raise exc_type("mock")
    if wait_time is not None:
        await asyncio.sleep(wait_time)
    if vs:
        return (v, *vs)
    return v


async def collect(iterator) -> list[Any]:
    return [item async for item in iterator]


class AsyncFutureExecutor:
    def submit(self, fn, *args, **kwargs):
        future = asyncio.get_running_loop().create_future()
        try:
            future.set_result(fn(*args, **kwargs))
        except Exception as exc:
            future.set_exception(exc)
        return future


class ResultOnlyExecutor:
    def submit(self, fn, *args, **kwargs):
        result = fn(*args, **kwargs)

        class Future:
            def done(self):
                return True

            def result(self, timeout=None):
                return result

        return Future()


@pytest.mark.asyncio
async def test_amap_sync_functions():
    assert await collect(amap(map_func, ())) == list(map(map_func, ()))
    assert await collect(amap(map_func, (0, 1, 2))) == list(map(map_func, (0, 1, 2)))
    assert await collect(amap(map_func, {0, 1, 2})) == list(map(map_func, {0, 1, 2}))
    assert await collect(amap(map_func, "012")) == list(map(map_func, "012"))
    assert await collect(amap(map_func, range(6), batch_size=0)) == list(range(6))
    assert await collect(amap(map_func, ((0, 0), (1, 1)))) == [(0, 0), (1, 1)]
    assert await collect(amap(map_func, (0, 1, 2), (3, 4))) == [(0, 3), (1, 4)]
    assert await collect(
        amap(map_func, ((0, 0), (1, 1), (2, 2)), ((3, 3), (4, 4)))
    ) == [((0, 0), (3, 3)), ((1, 1), (4, 4))]

    assert await collect(amap(map_func, range(6), batch_size=3)) == [
        [0, 1, 2],
        [3, 4, 5],
    ]
    assert await collect(amap(map_func, range(7), batch_size=3)) == [
        [0, 1, 2],
        [3, 4, 5],
        [6],
    ]
    assert await collect(amap(map_func, range(7), chunk_size=2)) == list(range(7))
    assert await collect(amap(map_func, range(7), batch_size=3, chunk_size=2)) == [
        [0, 1, 2],
        [3, 4, 5],
        [6],
    ]

    assert await collect(
        amap(map_func, (0, 1, 2), on_return=lambda x: cast(Any, x) + 1)
    ) == [
        1,
        2,
        3,
    ]
    assert await collect(amap(map_func, [Arguments(i, i) for i in range(2)])) == [
        (0, 0),
        (1, 1),
    ]

    # ordered by submission by default, unordered with sort_by_completion
    slow = [Arguments(i, wait_time=0.2 - 0.1 * i) for i in range(3)]
    assert await collect(amap(amap_func, slow)) == [0, 1, 2]
    assert await collect(amap(amap_func, slow, sort_by_completion=True)) == [2, 1, 0]


@pytest.mark.asyncio
async def test_amap_async_functions():
    assert await collect(amap(amap_func, (0, 1, 2))) == [0, 1, 2]
    assert await collect(amap(partial(amap_func, wait_time=0.0), (0, 1))) == [0, 1]

    class AsyncCallable:
        async def __call__(self, x):
            return x * 2

    assert await collect(amap(AsyncCallable(), (0, 1))) == [0, 2]

    def sync_returns_awaitable(x):
        return asyncio.sleep(0, result=x + 1)

    assert await collect(amap(sync_returns_awaitable, (0, 1))) == [1, 2]


@pytest.mark.asyncio
async def test_amap_async_iterables():
    assert await collect(amap(amap_func, aiter_range(3))) == [0, 1, 2]
    assert await collect(amap(amap_func, aiter_range(3), (3, 4, 5))) == [
        (0, 3),
        (1, 4),
        (2, 5),
    ]
    assert await collect(amap(amap_func, (0, 1, 2), aiter_range(3))) == [
        (0, 0),
        (1, 1),
        (2, 2),
    ]

    # strict works across sync and async iterables of any combination
    assert await collect(amap(amap_func, aiter_range(2), strict=True)) == [0, 1]
    assert await collect(amap(amap_func, aiter_range(2), (0,), strict=False)) == [
        (0, 0)
    ]

    with pytest.raises(ValueError) as excinfo:
        await collect(amap(amap_func, aiter_range(2), aiter_range(1), strict=True))
    assert str(excinfo.value) == "amap() argument 2 is shorter than argument 1"

    with pytest.raises(ValueError) as excinfo:
        await collect(amap(amap_func, aiter_range(1), aiter_range(2), strict=True))
    assert str(excinfo.value) == "amap() argument 2 is longer than argument 1"

    with pytest.raises(ValueError) as excinfo:
        await collect(
            amap(amap_func, aiter_range(2), aiter_range(2), aiter_range(1), strict=True)
        )
    assert str(excinfo.value) == ("amap() argument 3 is shorter than arguments 1-2")


@pytest.mark.asyncio
async def test_amap_batch_chunk_and_on_return():
    async def batch_fn(row):
        return [x * 2 for x in row]

    assert await collect(amap(batch_fn, range(5), batch_size=2)) == [
        [0, 2],
        [4, 6],
        [8],
    ]
    assert await collect(amap(amap_func, range(7), chunk_size=3)) == list(range(7))

    async def inc(x):
        await asyncio.sleep(0)
        return x + 1

    assert await collect(amap(map_func, range(3), on_return=inc)) == [1, 2, 3]
    assert await collect(amap(amap_func, range(4), chunk_size=2, on_return=inc)) == [
        1,
        2,
        3,
        4,
    ]

    async def fail_on_one(x):
        if x == 1:
            raise ValueError("on_return failed")
        return x

    with pytest.raises(ValueError, match="on_return failed"):
        await collect(amap(amap_func, range(3), on_return=fail_on_one))


@pytest.mark.asyncio
async def test_amap_executors():
    with ThreadPoolExecutor(max_workers=4) as executor:
        assert await collect(amap(map_func, range(6), executor=executor)) == list(
            range(6)
        )
        assert await collect(
            amap(map_func, range(6), executor=executor, chunk_size=2)
        ) == list(range(6))
        assert await collect(
            amap(
                amap_func,
                [Arguments(i, wait_time=0.2 - 0.1 * i) for i in range(3)],
                executor=executor,
                sort_by_completion=True,
            )
        ) == [2, 1, 0]

    assert await collect(
        amap(map_func, range(3), executor=cast(Any, AsyncFutureExecutor()))
    ) == [0, 1, 2]
    assert await collect(amap(map_func, range(3), executor=ResultOnlyExecutor())) == [
        0,
        1,
        2,
    ]


@pytest.mark.asyncio
async def test_amap_window():
    started = []

    async def fn(x):
        started.append(x)
        return x

    iterator = amap(fn, range(3), num_prepare=1)
    assert await iterator.__anext__() == 0
    assert started == [0]
    assert await iterator.__anext__() == 1
    assert started == [0, 1]
    assert await collect(iterator) == [2]
    assert started == [0, 1, 2]

    assert await collect(amap(amap_func, range(3), num_prepare=0)) == [0, 1, 2]


@pytest.mark.asyncio
async def test_amap_fail_fast():
    cancelled = asyncio.Event()

    async def fn(x):
        if x == 0:
            raise RuntimeError("mock")
        try:
            await asyncio.sleep(10)
        finally:
            cancelled.set()
        return x

    with pytest.raises(RuntimeError, match="mock"):
        await collect(amap(fn, range(2), num_prepare=2))
    assert cancelled.is_set()

    source_closed = asyncio.Event()

    async def source():
        try:
            for i in range(10):
                yield i
        finally:
            source_closed.set()

    async def boom(x):
        raise RuntimeError("mock")

    with pytest.raises(RuntimeError, match="mock"):
        await collect(amap(boom, source(), num_prepare=1))
    assert source_closed.is_set()


@pytest.mark.asyncio
async def test_amap_aclose_cancels_and_closes():
    cancelled = asyncio.Event()
    source_closed = asyncio.Event()

    async def source():
        try:
            for i in range(10):
                yield i
        finally:
            source_closed.set()

    async def fn(x):
        if x:
            try:
                await asyncio.sleep(10)
            finally:
                cancelled.set()
        return x

    async with contextlib.aclosing(
        cast(Any, amap(fn, source(), num_prepare=2))
    ) as iterator:
        async for value in iterator:
            assert value == 0
            break

    assert cancelled.is_set()
    assert source_closed.is_set()


def test_map_rejects_async_functions():
    async def async_fn(x):
        return x

    with pytest.raises(TypeError, match="use amap"):
        fmap(async_fn, (0,))
    with pytest.raises(TypeError, match="use amap"):
        fmap(map_func, (0,), on_return=async_fn)

    class AsyncCallable:
        async def __call__(self, x):
            return x

    with pytest.raises(TypeError, match="use amap"):
        fmap(AsyncCallable(), (0,))


def test_amap_validation():
    with pytest.raises(TypeError, match="iterable"):
        amap(map_func, cast(Any, 1))
    with pytest.raises(ValueError, match="num_prepare"):
        amap(map_func, (), num_prepare=-1)
