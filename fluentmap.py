"""
Fluentmap is a drop-in replacement for Python map featuring parallel and batch
processing.

Copyright (c) 2020-2024, Leavers.
License: MIT
"""

import asyncio
import inspect
import os
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Iterator,
)
from concurrent.futures import Future as ConcurrentFuture
from queue import Empty, Queue
from threading import Event, Thread
from typing import Any, Protocol, TypeVar, cast, overload, runtime_checkable

__version__ = "0.2.0"
__all__ = ("Arguments", "amap", "map")


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
R = TypeVar("R")


@runtime_checkable
class FutureLike(Protocol[T_co]):
    def done(self) -> bool: ...

    def result(self, timeout: Any = None) -> T_co: ...


@runtime_checkable
class SupportsSubmit(Protocol):
    def submit(*args: Any, **kwargs: Any) -> FutureLike: ...


class Arguments:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs

    def __eq__(self, obj: object) -> bool:
        if not isinstance(obj, Arguments):
            return False
        return self.args == obj.args and self.kwargs == obj.kwargs

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(args={self.args}, kwargs={self.kwargs})"


def _is_async_callable(fn: Any) -> bool:
    if inspect.iscoroutinefunction(fn):
        return True
    # inspect.iscoroutinefunction() does not look at __call__, so callable objects
    # with an async __call__ need a second check.
    call = getattr(fn, "__call__", None)
    return call is not None and inspect.iscoroutinefunction(call)


AnyIterable = Iterable[Any] | AsyncIterable[Any]


async def _anext(iterator: Any, is_async: bool) -> Any:
    if is_async:
        return await iterator.__anext__()
    try:
        return next(iterator)
    except StopIteration:
        raise StopAsyncIteration from None


async def _aiter_args(
    args: AnyIterable,
    *more_args: AnyIterable,
    strict: bool = False,
) -> AsyncIterator[tuple[Any, ...]]:
    sources: list[tuple[Any, bool]] = []
    async_iterators: list[AsyncIterator[Any]] = []
    for source in (args, *more_args):
        if isinstance(source, AsyncIterable):
            iterator = source.__aiter__()
            async_iterators.append(iterator)
            sources.append((iterator, True))
        else:
            sources.append((iter(cast(Iterable[Any], source)), False))

    try:
        if not strict:
            while True:
                values = []
                for iterator, is_async in sources:
                    try:
                        values.append(await _anext(iterator, is_async))
                    except StopAsyncIteration:
                        return
                yield tuple(values)
        else:
            # Same checks and messages as the strict builtin map, except for the
            # function name.
            while True:
                values = []
                for i, (iterator, is_async) in enumerate(sources):
                    try:
                        values.append(await _anext(iterator, is_async))
                    except StopAsyncIteration:
                        if i:
                            plural = " " if i == 1 else "s 1-"
                            raise ValueError(
                                f"amap() argument {i + 1} is shorter than "
                                f"argument{plural}{i}"
                            ) from None
                        for j, (other, other_is_async) in enumerate(
                            sources[1:], start=1
                        ):
                            try:
                                await _anext(other, other_is_async)
                            except StopAsyncIteration:
                                continue
                            plural = " " if j == 1 else "s 1-"
                            raise ValueError(
                                f"amap() argument {j + 1} is longer than "
                                f"argument{plural}{j}"
                            ) from None
                        return
                yield tuple(values)
    finally:
        for iterator in async_iterators:
            aclose = getattr(iterator, "aclose", None)
            if aclose is None:
                continue
            try:
                await aclose()
            except Exception:
                pass


async def _amap(
    fn: Callable[..., Any],
    args: AnyIterable,
    more_args: tuple[AnyIterable, ...],
    executor: SupportsSubmit | None,
    batch_size: int,
    chunk_size: int,
    window: int,
    sort_by_completion: bool,
    on_return: Callable[[Any], Any] | None,
    strict: bool,
    is_async_fn: bool,
) -> AsyncIterator[Any]:
    is_batch_args = batch_size > 1
    extract_args = bool(more_args)
    source = _aiter_args(args, *more_args, strict=strict)
    queue: asyncio.Queue = asyncio.Queue()
    inflight: set[asyncio.Task] = set()
    results: dict[int, Any] = {}
    next_submit = 0
    next_yield = 0
    has_more = True
    batch: list[Any] = []
    chunk: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def make_payload(
        fn_args: Any,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        if is_batch_args:
            return ((fn_args,), {})
        if isinstance(fn_args, Arguments):
            return (fn_args.args, fn_args.kwargs)
        if extract_args and isinstance(fn_args, Iterable):
            return (tuple(fn_args), {})
        return ((fn_args,), {})

    async def call(call_args: tuple[Any, ...], call_kwargs: dict[str, Any]) -> Any:
        if is_async_fn:
            result = await fn(*call_args, **call_kwargs)
        elif executor is None:
            result = await asyncio.to_thread(fn, *call_args, **call_kwargs)
        else:
            future = executor.submit(fn, *call_args, **call_kwargs)
            if isinstance(future, asyncio.Future):
                result = await cast(Awaitable[Any], future)
            elif isinstance(future, ConcurrentFuture):
                result = await asyncio.wrap_future(
                    cast("ConcurrentFuture[Any]", future)
                )
            else:
                result = await asyncio.to_thread(future.result)
        if inspect.isawaitable(result):
            result = await cast(Awaitable[Any], result)
        return result

    async def run_task(
        indices: list[int],
        payloads: list[tuple[tuple[Any, ...], dict[str, Any]]],
    ) -> None:
        try:
            entries: list[tuple[int, Any]] = []
            for index, (call_args, call_kwargs) in zip(indices, payloads):
                try:
                    value = await call(call_args, call_kwargs)
                    if on_return is not None:
                        value = on_return(value)
                        if inspect.isawaitable(value):
                            value = await cast(Awaitable[Any], value)
                except BaseException as exc:
                    queue.put_nowait((None, exc))
                    return
                entries.append((index, value))
            queue.put_nowait((entries, None))
        finally:
            inflight.discard(asyncio.current_task())

    def submit_chunk(
        payloads: list[tuple[tuple[Any, ...], dict[str, Any]]],
    ) -> None:
        nonlocal next_submit
        indices = list(range(next_submit, next_submit + len(payloads)))
        next_submit += len(payloads)
        task = asyncio.create_task(run_task(indices, list(payloads)))
        inflight.add(task)

    try:
        while has_more or next_yield < next_submit:
            while has_more and (window == 0 or len(inflight) + len(results) < window):
                try:
                    fn_args = await source.__anext__()
                    if not extract_args:
                        fn_args = fn_args[0]
                except StopAsyncIteration:
                    has_more = False
                    if is_batch_args and batch:
                        chunk.append(make_payload(batch))
                        batch = []
                    if chunk:
                        submit_chunk(chunk)
                        chunk = []
                    break

                if is_batch_args:
                    batch.append(fn_args)
                    if len(batch) < batch_size:
                        continue
                    chunk.append(make_payload(batch))
                    batch = []
                else:
                    chunk.append(make_payload(fn_args))

                if chunk_size <= 1 or len(chunk) >= chunk_size:
                    submit_chunk(chunk)
                    chunk = []

            if not inflight and next_yield >= next_submit:
                break

            entries, error = await queue.get()
            if error is not None:
                raise error
            for index, value in entries:
                if sort_by_completion:
                    next_yield += 1
                    yield value
                else:
                    results[index] = value
                    while next_yield in results:
                        yield results.pop(next_yield)
                        next_yield += 1
    finally:
        pending = list(inflight)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        close_source = getattr(source, "aclose", None)
        if close_source is not None:
            await close_source()


def _zip_args(
    args: Iterable[Any],
    *more_args: Iterable[Any],
    strict: bool = False,
) -> Iterator[tuple[Any, ...]]:
    if not strict:
        yield from zip(args, *more_args)
        return

    # Same checks and messages as the strict builtin map (zip(strict=True) is only
    # available on Python 3.10+).
    iterators = [iter(args), *(iter(it) for it in more_args)]
    while True:
        values = []
        for i, iterator in enumerate(iterators):
            try:
                values.append(next(iterator))
            except StopIteration:
                if i:
                    plural = " " if i == 1 else "s 1-"
                    raise ValueError(
                        f"map() argument {i + 1} is shorter than argument{plural}{i}"
                    ) from None
                for j, other in enumerate(iterators[1:], start=1):
                    try:
                        next(other)
                    except StopIteration:
                        continue
                    plural = " " if j == 1 else "s 1-"
                    raise ValueError(
                        f"map() argument {j + 1} is longer than argument{plural}{j}"
                    ) from None
                return
        yield tuple(values)


def _simple_map(
    fn: Callable[..., T],
    args: Iterable[Any],
    *more_args: Iterable[Any],
    batch_size: int = 0,
    num_prepare: int = 0,
    on_return: Callable[[T], R] | None = None,
    strict: bool = False,
) -> Iterator[T | R]:
    is_batch_args = True if batch_size > 1 else False

    if more_args:
        iterable_args = cast(Iterable[Any], _zip_args(args, *more_args, strict=strict))
        extract_args = True
    else:
        iterable_args = args
        extract_args = False

    def submit(fn_args: Arguments | Iterable[Any] | Any) -> T | R:
        if is_batch_args:
            res = fn(fn_args)
        else:
            if isinstance(fn_args, Arguments):
                res = fn(*fn_args.args, **fn_args.kwargs)
            elif isinstance(fn_args, Iterable) and extract_args:
                res = fn(*fn_args)
            else:
                res = fn(fn_args)
        if on_return is None:
            return res
        return on_return(res)

    if num_prepare == 0:
        batch: list[Any] = []
        for args in iterable_args:
            if is_batch_args:
                batch.append(args)
                if len(batch) < batch_size:
                    continue
                yield submit(batch)
                batch = []
            else:
                yield submit(args)
        if batch:
            yield submit(batch)
    else:
        undefined = object()
        queue: Queue = Queue()
        exc_event = Event()
        worker_exc: list[BaseException] = []

        def worker():
            batch: list[Any] = []
            try:
                for args in iterable_args:
                    if exc_event.is_set():
                        break
                    if is_batch_args:
                        batch.append(args)
                        if len(batch) < batch_size:
                            continue
                        queue.put(batch)
                        batch = []
                    else:
                        queue.put(args)
                        del args
                    while queue.qsize() >= num_prepare:
                        if exc_event.is_set():
                            break
                        exc_event.wait(0.05)
                if not exc_event.is_set() and batch:
                    queue.put(batch)
            except BaseException as exc:
                worker_exc.append(exc)
            finally:
                queue.put(undefined)

        worker_thread = Thread(target=worker, daemon=True)
        worker_thread.start()
        while True:
            try:
                data = queue.get(timeout=0.05)
            except Empty:
                continue
            if data is undefined:
                if worker_exc:
                    raise worker_exc[0]
                break
            try:
                yield submit(data)
            except Exception as exc:
                exc_event.set()
                worker_thread.join()
                raise exc
            finally:
                del data
        worker_thread.join()


def _process_chunk(
    fn: Callable,
    fn_args_chunk: list[Arguments | Iterable[Any] | Any],
    *,
    extract_args: bool,
    is_batch_args: bool,
) -> list[tuple[Any, bool]]:
    result: list[tuple[Any, bool]] = []
    if is_batch_args:
        for fn_args in fn_args_chunk:
            try:
                ret = fn(fn_args)
            except BaseException as exc:
                result.append((exc, True))
            else:
                result.append((ret, False))
    else:
        for fn_args in fn_args_chunk:
            try:
                if isinstance(fn_args, Arguments):
                    ret = fn(*fn_args.args, **fn_args.kwargs)
                elif isinstance(fn_args, Iterable) and extract_args:
                    ret = fn(*fn_args)
                else:
                    ret = fn(fn_args)
            except BaseException as exc:
                result.append((exc, True))
            else:
                result.append((ret, False))
    return result


def _concurrent_chunk_map(
    fn: Callable[..., T],
    args: Iterable[Any],
    *more_args: Iterable[Any],
    executor: SupportsSubmit,
    chunk_size: int,
    batch_size: int = 0,
    num_prepare: int = 0,
    sort_by_completion: bool = False,
    on_return: Callable[[T], R] | None = None,
    strict: bool = False,
) -> Iterator[T | R]:
    assert chunk_size > 1
    is_batch_args = True if batch_size > 1 else False

    args_iter: Iterator[Any]
    if more_args:
        args_iter = iter(_zip_args(args, *more_args, strict=strict))

        # Whether to deconstruct iterable args in submit, this helps sumbit identify
        # whether an iterable fn_args is a batch generated by _concurrent_map itself
        # (which should be deconstructed) or a item in args given by users (which
        # should not be deconstructed)
        extract_args = True
    else:
        args_iter = iter(args)
        extract_args = False

    # Indicates the chunk currently being assembled
    chunk: list[Any] = []

    # The chunk sequence number which is auto-incremental and unique.
    chunk_cursor = 0

    # Indicates the batch currently being assembled. If batch_size is 0, the length
    # of batch will only be 1 and will be deconstructed before being added to
    # pending_batches.
    batch: list[Any] = []

    # Batch cursors which have been submitted but not yielded yet.
    task_indices: set[int] = set()

    # Futures which have not been yielded.
    futures: dict[int, FutureLike[list[tuple[T | BaseException, bool]]]] = {}

    # Store results when sort_by_completion is False to make sure results are returned
    # in the order of submission.
    result_cache: dict[int, list[tuple[T | BaseException, bool]]] = {}

    # Used to reorder results and communicate with handle_result.
    result_queue: Queue = Queue()

    # Indicates the maximum index of result that has been processed by handle_result
    # when sort_by_completion is False
    result_cursor = 0

    # Indicates the maximum index of result that has been returned when
    # sort_by_completion is False
    result_cursor_bound = 0

    def submit(
        fn_args_chunk: list[Arguments | Iterable[Any] | Any],
    ) -> FutureLike[list[tuple[T | BaseException, bool]]]:
        return executor.submit(
            _process_chunk,
            fn,
            fn_args_chunk,
            extract_args=extract_args,
            is_batch_args=is_batch_args,
        )

    if on_return is None:

        def handle_result_chunk(
            i: int,
            result_chunk: list[tuple[T | BaseException, bool]],
        ) -> None:
            nonlocal result_cursor, result_cursor_bound

            if sort_by_completion:
                # yield result in the order of completion
                result_queue.put((i, result_chunk))
            else:
                # yield result in the order of submission
                result_cursor_bound = max(result_cursor_bound, i)
                result_cache[i] = result_chunk
                while result_cursor <= result_cursor_bound:
                    if result_cursor not in result_cache:
                        return
                    result_chunk = result_cache.pop(result_cursor)
                    result_queue.put((result_cursor, result_chunk))
                    result_cursor += 1

        def stop_result_handler() -> None: ...

    else:

        def handle_result_concurrently(inqueue: Queue, outqueue: Queue) -> None:
            while True:
                try:
                    pair: tuple[int, list[tuple[T | BaseException, bool]]] | None = (
                        inqueue.get(timeout=0.01)
                    )
                except Empty:
                    continue

                if pair is None:
                    break

                i, raw_result_chunk = pair
                results: list[tuple[T | R | BaseException, bool]] = []
                for raw_result, is_exc in raw_result_chunk:
                    if is_exc:
                        results.append((raw_result, True))
                        continue
                    try:
                        new_result = on_return(cast(T, raw_result))
                        results.append((new_result, False))
                    except BaseException as exc:
                        results.append((exc, True))
                        del exc
                    finally:
                        del raw_result
                outqueue.put((i, results))
                del pair

        raw_result_queue: Queue = Queue()
        result_handler: Thread | None = None

        def stop_result_handler() -> None:
            nonlocal result_handler
            if result_handler is None or not result_handler.is_alive():
                return
            raw_result_queue.put(None)
            result_handler.join()
            result_handler = None

        def handle_result_chunk(
            i: int,
            result_chunk: list[tuple[T | BaseException, bool]],
        ) -> None:
            nonlocal result_handler, result_cache
            nonlocal result_cursor, result_cursor_bound

            if result_handler is None:
                result_handler = Thread(
                    target=handle_result_concurrently,
                    args=(raw_result_queue, result_queue),
                    daemon=True,
                )
                result_handler.start()

            if sort_by_completion:
                # yield result in the order of completion
                raw_result_queue.put((i, result_chunk))
            else:
                # yield result in the order of submission
                result_cursor_bound = max(result_cursor_bound, i)
                result_cache[i] = result_chunk
                while result_cursor <= result_cursor_bound:
                    if result_cursor not in result_cache:
                        return
                    result = result_cache.pop(result_cursor)
                    raw_result_queue.put((result_cursor, result))
                    result_cursor += 1

    try:
        # Indicates whether there are pending data in args_iter. At least one
        # iteration should be invoked so the initial value is True and it will be set
        # as False on StopIteration.
        has_args = True

        # Conditions to stop iteration (must meet all requirements):
        # 1. No more data in args_iter (has_args is False)
        # 2. The last batch has been submitted (batch is empty)
        # 3. All results have been yielded (task_indices is empty)
        while has_args or chunk or batch or task_indices:
            if has_args and (num_prepare == 0 or len(futures) < num_prepare):
                try:
                    fn_args = next(args_iter)
                    if is_batch_args:
                        batch.append(fn_args)
                        if len(batch) >= batch_size:
                            chunk.append(batch)
                            batch = []
                            if len(chunk) >= chunk_size:
                                futures[chunk_cursor] = submit(chunk)
                                task_indices.add(chunk_cursor)
                                chunk_cursor += 1
                                chunk = []
                    else:
                        chunk.append(fn_args)
                        if len(chunk) >= chunk_size:
                            futures[chunk_cursor] = submit(chunk)
                            task_indices.add(chunk_cursor)
                            chunk_cursor += 1
                            chunk = []
                except StopIteration:
                    if is_batch_args and batch:
                        chunk.append(batch)
                        batch = []
                    if chunk:
                        futures[chunk_cursor] = submit(chunk)
                        task_indices.add(chunk_cursor)
                        chunk_cursor += 1
                        chunk = []
                    has_args = False

            if futures:
                finished_indices: list[int] = []
                for i, future in futures.items():
                    if not future.done():
                        continue
                    finished_indices.append(i)
                    handle_result_chunk(i, future.result())
                for i in finished_indices:
                    futures.pop(i)

            while not result_queue.empty():
                i, results = result_queue.get()
                task_indices.remove(i)
                for result, is_exc in results:
                    if is_exc:
                        stop_result_handler()
                        raise cast(BaseException, result)
                    else:
                        yield result
    finally:
        stop_result_handler()


def _concurrent_map(
    fn: Callable[..., T],
    args: Iterable[Any],
    *more_args: Iterable[Any],
    executor: SupportsSubmit,
    batch_size: int = 0,
    num_prepare: int = 0,
    sort_by_completion: bool = False,
    on_return: Callable[[T], R] | None = None,
    strict: bool = False,
) -> Iterator[T | R]:
    is_batch_args = True if batch_size > 1 else False

    args_iter: Iterator[Any]
    if more_args:
        args_iter = iter(_zip_args(args, *more_args, strict=strict))

        # Whether to deconstruct iterable args in submit, this helps sumbit identify
        # whether an iterable fn_args is a batch generated by _concurrent_map itself
        # (which should be deconstructed) or a item in args given by users (which
        # should not be deconstructed)
        extract_args = True
    else:
        args_iter = iter(args)
        extract_args = False

    # Indicates the batch currently being assembled. If batch_size is 0, the length
    # of batch will only be 1 and will be deconstructed before being added to
    # pending_batches.
    batch: list[Any] = []

    # The batch sequence number which is auto-incremental and unique.
    batch_cursor = 0

    # Batch cursors which have been submitted but not yielded yet.
    task_indices: set[int] = set()

    # Futures which have not been yielded.
    futures: dict[int, FutureLike[T]] = {}

    # Store results when sort_by_completion is False to make sure results are returned
    # in the order of submission.
    result_cache: dict[int, T] = {}

    # Used to reorder results and communicate with handle_result.
    result_queue: Queue = Queue()

    # Indicates the maximum index of result that has been processed by handle_result
    # when sort_by_completion is False
    result_cursor = 0

    # Indicates the maximum index of result that has been returned when
    # sort_by_completion is False
    result_cursor_bound = 0

    def submit(
        fn_args: Arguments | Iterable[Any] | Any,
    ) -> FutureLike[T]:
        if is_batch_args:
            future = executor.submit(fn, fn_args)
        else:
            if isinstance(fn_args, Arguments):
                future = executor.submit(fn, *fn_args.args, **fn_args.kwargs)
            elif isinstance(fn_args, Iterable) and extract_args:
                future = executor.submit(fn, *fn_args)
            else:
                future = executor.submit(fn, fn_args)
        return future

    if on_return is None:

        def handle_result(i: int, result: T) -> None:
            nonlocal result_cursor, result_cursor_bound

            if sort_by_completion:
                # yield result in the order of completion
                result_queue.put((i, result, False))
            else:
                # yield result in the order of submission
                result_cursor_bound = max(result_cursor_bound, i)
                result_cache[i] = result
                while result_cursor <= result_cursor_bound:
                    if result_cursor not in result_cache:
                        return
                    result = result_cache.pop(result_cursor)
                    result_queue.put((result_cursor, result, False))
                    result_cursor += 1

        def stop_result_handler() -> None: ...

    else:

        def handle_result_concurrently(inqueue: Queue, outqueue: Queue) -> None:
            while True:
                try:
                    pair: tuple[int, T] | None = inqueue.get(timeout=0.01)
                except Empty:
                    continue

                if pair is None:
                    break

                i, raw_result = pair
                try:
                    result = on_return(raw_result)
                    outqueue.put((i, result, False))
                except BaseException as exc:
                    outqueue.put((i, exc, True))
                    del exc
                finally:
                    del raw_result, pair

        raw_result_queue: Queue = Queue()
        result_handler: Thread | None = None

        def stop_result_handler() -> None:
            nonlocal result_handler
            if result_handler is None or not result_handler.is_alive():
                return
            raw_result_queue.put(None)
            result_handler.join()
            result_handler = None

        def handle_result(i: int, result: T) -> None:
            nonlocal result_handler, result_cache
            nonlocal result_cursor, result_cursor_bound

            if result_handler is None:
                result_handler = Thread(
                    target=handle_result_concurrently,
                    args=(raw_result_queue, result_queue),
                    daemon=True,
                )
                result_handler.start()

            if sort_by_completion:
                # yield result in the order of completion
                raw_result_queue.put((i, result))
            else:
                # yield result in the order of submission
                result_cursor_bound = max(result_cursor_bound, i)
                result_cache[i] = result
                while result_cursor <= result_cursor_bound:
                    if result_cursor not in result_cache:
                        return
                    result = result_cache.pop(result_cursor)
                    raw_result_queue.put((result_cursor, result))
                    result_cursor += 1

    try:
        # Indicates whether there are pending data in args_iter. At least one
        # iteration should be invoked so the initial value is True and it will be set
        # as False on StopIteration.
        has_args = True

        # Conditions to stop iteration (must meet all requirements):
        # 1. No more data in args_iter (has_args is False)
        # 2. The last batch has been submitted (batch is empty)
        # 3. All results have been yielded (task_indices is empty)
        while has_args or batch or task_indices:
            if has_args and (num_prepare == 0 or len(futures) < num_prepare):
                try:
                    fn_args = next(args_iter)
                    batch.append(fn_args)
                    if len(batch) >= batch_size:
                        futures[batch_cursor] = submit(
                            batch if is_batch_args else batch[0]
                        )
                        task_indices.add(batch_cursor)
                        batch_cursor += 1
                        batch = []
                except StopIteration:
                    if batch:
                        futures[batch_cursor] = submit(
                            batch if is_batch_args else batch[0]
                        )
                        task_indices.add(batch_cursor)
                        batch_cursor += 1
                        batch = []
                    has_args = False

            if futures:
                finished_indices: list[int] = []
                for i, future in futures.items():
                    if not future.done():
                        continue
                    finished_indices.append(i)
                    handle_result(i, future.result())
                for i in finished_indices:
                    futures.pop(i)

            while not result_queue.empty():
                i, result, is_exc = result_queue.get()
                task_indices.remove(i)
                if is_exc:
                    stop_result_handler()
                    raise cast(BaseException, result)
                else:
                    yield result
    finally:
        stop_result_handler()


@overload
def map(
    fn: Callable[..., T],
    args: Iterable[Any],
    *more_args: Iterable[Any],
    executor: SupportsSubmit | None = None,
    batch_size: int | None = None,
    chunk_size: int | None = None,
    num_prepare: int | None = None,
    sort_by_completion: bool = False,
    strict: bool = False,
) -> Iterator[T]: ...


@overload
def map(
    fn: Callable[..., T],
    args: Iterable[Any],
    *more_args: Iterable[Any],
    executor: SupportsSubmit | None = None,
    batch_size: int | None = None,
    chunk_size: int | None = None,
    num_prepare: int | None = None,
    sort_by_completion: bool = False,
    on_return: Callable[[T], R],
    strict: bool = False,
) -> Iterator[R]: ...


def map(
    fn: Callable[..., T],
    args: Iterable[Any],
    *more_args: Iterable[Any],
    executor: SupportsSubmit | None = None,
    batch_size: int | None = None,
    chunk_size: int | None = None,
    num_prepare: int | None = None,
    sort_by_completion: bool = False,
    on_return: Callable[[T], R] | None = None,
    strict: bool = False,
) -> Iterator[T | R]:
    """Drop-in replacement of builtin ``map`` but adds an argument ``executor`` for
    executing function in an executor.

    :type fn: Callable[..., T]
    :param fn: The function to be mapped.

    :type args: Iterable[Any]
    :param args: The arguments to be mapped into ``fn``.

    :type executor: SupportsSubmit, optional, default to ``None``
    :param executor: The executor to run function ``fn``. It supports not only an
        implementation of ``concurrent.futures.Executor`` but also any instances that
        have a method ``submit(fn, *args, **kwargs) -> Future``. The default value is
        ``None``, which means not to use an executor and this function will behave
        as the builtin ``map`` does.

    :type batch_size: int, optional, default to ``None``
    :param batch_size: If batch size is given, then items in args will be aranged into
        batches (a list of items) firstly before sent to ``fn``. For example,
        ``map(fn, (1, 2, 3, 4, 5), batch_size=2)`` means invoking 3 functions:
        ``fn([1, 2])``, ``fn([3, 4])`` and ``fn([5])``. Batch will be disabled if
        ``batch_size`` is set as ``None`` or a number which is <= 1.

    :type chunk_size: int, optional, default to ``None``
    :param chunk_size: If ``chunk_size`` is given, multiple arguments will be submitted
        to the executor in chunks and will be executed sequentially in one executor
        worker. ``chunk_size`` may be useful to reduce the time spent on worker
        initialization.

        Here is an example to illustrate the difference between ``batch_size`` and
        ``chunk_size``. Considering
        ``map(fn, (1, 2, 3, 4, 5), executor=some_executor)``:

        *. If set ``batch_size=2``, ``map`` will send 3 batched invocations to executor,
            ``fn([1, 2])``, ``fn([3, 4])`` and ``fn([5])``, so ``fn`` needs to accept
            list of integers as its arguments instead of single integer.
        *. If set ``chunk_size=2``, ``map`` will send 3 chunked invocations to executor,
            it firstly send ``1`` and ``2`` to a worker, the worker will invoke
            ``fn(1)`` and ``fn(2)`` sequentially; then ``3`` and ``4`` to another
            worker; then finally ``5``. This keeps ``fn`` not to be refactored to accept
            batch input.
        *. ``batch_size`` and ``chunk_size`` can be used simultaneously. If set
            ``batch_size=2`` and ``chunk_size=3``, then ``[1, 2]``, ``[3, 4]`` and
            ``[5]`` will be sent to a worker in on chunk.

        Note that ``chunk_size`` will be ignored if ``executor`` is ``None``.

    :type num_prepare: int, optional, default to ``None``
    :param num_prepare: Number of items or batches (if ``batch_size`` is enabled) to
        be submitted to executor. This can be useful to prevent get too many data
        from ``args`` and occupy huge memory. A typical usecase is setting
        ``num_prepare`` to a rational value if ``args`` is a iterable line reader
        of a super huge text file. If executor is ``None``, and ``num_prepare`` is
        larger than zero, a thread for yielding data will be started to non-blockingly
        fetch the specified amount of items or batches.

    :type sort_by_completion: bool, default to ``False``
    :param sort_by_completion: If set to ``True``, then the result will be returned in
        the order of their completion time. Otherwise returned in the order of
        submission, which is the same as builtin ``map``.

    :type on_return: Callable[[T], U], optional, default to ``None``
    :param on_return: An optional callback to be invoked on each result being returned.
        It accept each result as its sole argument and its return will be treated as
        as the final return of each invocation of ``fn``.

    :type strict: bool, optional, default to ``False``
    :param strict: If set to ``True``, raise ``ValueError`` when the iterables have
        different lengths, same as the strict builtin ``map``.

    :rtype: Iterator
    :return: An iterator which yield all results of ``fn``.
    """

    for fn_args in (args, *more_args):
        if not isinstance(fn_args, Iterable):
            raise TypeError(
                "Positional arguments after function should be iterable, "
                f"got {fn_args} ({type(fn_args)}) instead."
            )
    if _is_async_callable(fn):
        raise TypeError("map() does not support async functions, use amap() instead")
    if on_return is not None and _is_async_callable(on_return):
        raise TypeError("map() does not support async on_return, use amap() instead")
    if batch_size is None or batch_size <= 1:
        batch_size = 0
    if num_prepare is None or num_prepare <= 0:
        num_prepare = 0

    if executor is None:
        return _simple_map(
            fn,
            args,
            *more_args,
            batch_size=batch_size,
            num_prepare=num_prepare,
            on_return=on_return,
            strict=strict,
        )

    if chunk_size is None or chunk_size <= 1:
        return _concurrent_map(
            fn,
            args,
            *more_args,
            executor=executor,
            batch_size=batch_size,
            num_prepare=num_prepare,
            sort_by_completion=sort_by_completion,
            on_return=on_return,
            strict=strict,
        )
    return _concurrent_chunk_map(
        fn,
        args,
        *more_args,
        executor=executor,
        batch_size=batch_size,
        chunk_size=chunk_size,
        num_prepare=num_prepare,
        sort_by_completion=sort_by_completion,
        on_return=on_return,
        strict=strict,
    )


@overload
def amap(
    fn: Callable[..., T | Awaitable[T]],
    args: AnyIterable,
    *more_args: AnyIterable,
    executor: SupportsSubmit | None = None,
    batch_size: int | None = None,
    chunk_size: int | None = None,
    num_prepare: int | None = None,
    sort_by_completion: bool = False,
    strict: bool = False,
) -> AsyncIterator[T]: ...


@overload
def amap(
    fn: Callable[..., T | Awaitable[T]],
    args: AnyIterable,
    *more_args: AnyIterable,
    executor: SupportsSubmit | None = None,
    batch_size: int | None = None,
    chunk_size: int | None = None,
    num_prepare: int | None = None,
    sort_by_completion: bool = False,
    on_return: Callable[[T], R | Awaitable[R]],
    strict: bool = False,
) -> AsyncIterator[R]: ...


def amap(
    fn: Callable[..., T | Awaitable[T]],
    args: AnyIterable,
    *more_args: AnyIterable,
    executor: SupportsSubmit | None = None,
    batch_size: int | None = None,
    chunk_size: int | None = None,
    num_prepare: int | None = None,
    sort_by_completion: bool = False,
    on_return: Callable[[T], R | Awaitable[R]] | None = None,
    strict: bool = False,
) -> AsyncIterator[T | R]:
    """Asynchronous counterpart of :func:`map`.

    Unlike :func:`map`, ``fn`` may be either a synchronous or an asynchronous
    function. Synchronous functions are executed in the default thread pool (or in
    ``executor`` when given), while asynchronous functions are awaited directly on
    the running event loop. It is also possible to pass asynchronous iterables.

    :type fn: Callable[..., T | Awaitable[T]]
    :param fn: The function to be mapped. It may return either a value or an
        awaitable.

    :type args: Iterable[Any] | AsyncIterable[Any]
    :param args: The arguments to be mapped into ``fn``. Positional arguments after
        the function may be synchronous iterables, asynchronous iterables, or any
        mix of both.

    :type executor: SupportsSubmit, optional, default to ``None``
    :param executor: The executor used for synchronous ``fn``. Any object with a
        method ``submit(fn, *args, **kwargs) -> Future`` is accepted. ``executor``
        is ignored when ``fn`` is an asynchronous function. The default value is
        ``None``, which means to use the event loop's default thread pool.

    :type batch_size: int, optional, default to ``None``
    :param batch_size: Same as :func:`map`.

    :type chunk_size: int, optional, default to ``None``
    :param chunk_size: Same as :func:`map`, except that it also takes effect when
        ``executor`` is ``None``.

    :type num_prepare: int, optional, default to ``None``
    :param num_prepare: Maximum number of tasks that are allowed to be in flight at
        the same time. If set to ``None``, a default window of
        ``min(32, cpu_count + 4)`` is used. ``0`` means no limit. A negative value
        raises ``ValueError``.

    :type sort_by_completion: bool, default to ``False``
    :param sort_by_completion: If set to ``True``, results are yielded in the order
        of completion. Otherwise they are yielded in submission order.

    :type on_return: Callable[[T], R | Awaitable[R]], optional, default to ``None``
    :param on_return: An optional callback invoked on each result. It may be a
        synchronous or an asynchronous function. When ``chunk_size`` is used, it
        is invoked once per item instead of once per chunk.

    :type strict: bool, optional, default to ``False``
    :param strict: If set to ``True``, raise ``ValueError`` when the iterables have
        different lengths. The error messages use the ``amap()`` prefix.

    :rtype: AsyncIterator
    :return: An asynchronous iterator yielding all results of ``fn``.

    .. note::
        Results are yielded fail-fast: the first task that raises an exception
        cancels all in-flight tasks and the exception is re-raised from the
        iterator. Breaking out of ``async for`` does not close the iterator
        automatically; use ``contextlib.aclosing()`` or call ``aclose()`` to cancel
        pending tasks and close asynchronous iterables.
    """

    for fn_args in (args, *more_args):
        if not isinstance(fn_args, (Iterable, AsyncIterable)):
            raise TypeError(
                "Positional arguments after function should be iterable, "
                f"got {fn_args} ({type(fn_args)}) instead."
            )
    if batch_size is None or batch_size <= 1:
        batch_size = 0
    if chunk_size is None or chunk_size <= 1:
        chunk_size = 0
    if num_prepare is None:
        window = min(32, (os.cpu_count() or 1) + 4)
    elif num_prepare < 0:
        raise ValueError("num_prepare must be greater than or equal to 0")
    else:
        window = num_prepare

    return _amap(
        fn,
        args,
        more_args,
        executor=executor,
        batch_size=batch_size,
        chunk_size=chunk_size,
        window=window,
        sort_by_completion=sort_by_completion,
        on_return=on_return,
        strict=strict,
        is_async_fn=_is_async_callable(fn),
    )
