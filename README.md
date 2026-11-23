# Fluentmap

[![Testing](https://github.com/leavers/fluentmap/actions/workflows/test-suite.yml/badge.svg)](https://github.com/leavers/fluentmap/actions/workflows/test-suite.yml)
[![Package version](https://img.shields.io/pypi/v/fluentmap.svg)](https://pypi.org/project/fluentmap/)
[![Python](https://img.shields.io/pypi/pyversions/fluentmap.svg)](https://pypi.org/project/fluentmap/)

Fluentmap provides a drop-in Python map replacement featuring parallel and batch
processing.

## Features

- Use `executor` to run tasks in parallel.
- Use `amap` to await synchronous or asynchronous functions with `async for`.
- Use `batch_size`/`chunk_size` to send parameters in batches/chunks.
- Use `num_prepare` to prepare data in advance for better performance.
- Call `on_return` hook to process the return value of each task.
- Support the builtin `strict` keyword.

## Installation

Fluentmap requires Python 3.11 or newer, has no runtime dependencies, and is tested
on CPython 3.11-3.15, including the free-threaded builds (3.13t-3.15t).

Fluentmap is available on [PyPI](https://pypi.org/project/fluentmap/):

```shell
pip install fluentmap
```

## Usage

### Drop-in replacement

You can start to use fluentmap just like built-in `map`:

```python
from typing import Any

from fluentmap import map


items: list[str] = [...]


def heavy_task(item: str) -> Any:
    """Suppose this function represents a computationally expensive task."""


def postprocessing(result: Any):
    """Suppose this function represents a postprocessing task."""


for result in map(heavy_task, items):
    postprocessing(result)
```

### Parallel processing

As `heavy_task` is a computationally expensive task, you can use `executor` to
run it in parallel.

```python
from concurrent.futures import ProcessPoolExecutor

from fluentmap import map

# ......

with ProcessPoolExecutor() as executor:
    # each heavy_task invocation runs in a separate process
    for result in map(heavy_task, items, executor=executor):
        postprocessing(result)
```

### Batch/chunk processing

You can use `batch_size`/`chunk_size` to send arguments in batches/chunks.

The difference between them is that `batch_size` packs multiple arguments into a batch
before sending them to the function, therefore the function needs to be modified to
handle a list of arguments.

On the other hand, `chunk_size` packs multiple arguments into a chunks before passing
them to executor workers, while workers still process each argument sequentially.

```python
from concurrent.futures import ProcessPoolExecutor
from typing import Any

from fluentmap import map


# ......


def heavy_task_in_batch(item: list[str]) -> Any:
    """Note that `item` is a list since when `batch_size` is set,
    fluentmap will concatenate multiple items into a batch before sending them to
    the function which is to be invoked.
    """


# An example of using `batch_size`
with ProcessPoolExecutor() as executor:
    for result in map(
        heavy_task_in_batch,
        items,
        executor=executor,
        batch_size=64,
    ):
        postprocessing(result)

# An example of using `chunk_size` 
with ProcessPoolExecutor() as executor:
    for result in map(
        heavy_task,
        items,
        executor=executor,
        chunk_size=32,
    ):
        postprocessing(result)
```

### Async processing

Use `amap` when the work is asynchronous. Its signature is the same as `map`, but
`fn` may be a regular function or an `async def` function, and the input may be
any mix of synchronous and asynchronous iterables.

```python
import asyncio
import contextlib
from typing import Any

from fluentmap import amap


items: list[str] = [...]


async def fetch(item: str) -> Any:
    """Suppose this function performs an asynchronous request."""


async def main() -> None:
    async with contextlib.aclosing(amap(fetch, items)) as results:
        async for result in results:
            postprocessing(result)


asyncio.run(main())
```

`amap` is a coroutine-based scheduler: asynchronous functions are awaited directly,
while synchronous functions run in the event loop's default thread pool or in
`executor` when one is given. It accepts the same `batch_size`, `chunk_size`,
`num_prepare`, `sort_by_completion`, `on_return` and `strict` options as `map`.
`num_prepare` defaults to `min(32, cpu_count + 4)` in-flight tasks; set it to `0`
to remove the limit. `map` itself rejects async functions and async `on_return`
with a `TypeError` pointing to `amap`.

Results are yielded in submission order, or in completion order with
`sort_by_completion=True`. The first exception cancels all pending tasks and is
re-raised from the iterator. Breaking out of `async for` does not close the
iterator automatically, so use `contextlib.aclosing()` or call `aclose()` to cancel
pending work and close asynchronous iterables.
