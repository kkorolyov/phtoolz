import io
from contextlib import contextmanager
from datetime import date
from time import mktime
from typing import Any, Protocol, TypeVar

import requests

S = TypeVar("S", bound=str)
T = TypeVar("T", covariant=True)


class Numeric(Protocol[T]):
    def __round__(self, digits: int, /) -> T: ...
    def __float__(self, /) -> float: ...


N = TypeVar("N", bound=Numeric[Any])


@contextmanager
def client(url: str):
    """Returns a metrics client sending metrics to `url`."""
    res = Promport(url)
    try:
        yield res
    finally:
        res.flush()


class Promport:
    """Pushes metrics to Prometheus."""

    _url: str
    _buffer: list[str]

    def __init__(self, url: str) -> None:
        self._url = url
        self._buffer = []

    def delete(self, pattern: str):
        """Deletes samples for timeseries matching name `pattern`."""

        requests.post(
            f"{self._url}/delete",
            {"match[]": f'{{__name__=~"{pattern}"}}'},
        ).raise_for_status()

        print(f"deleted metrics matching {pattern} at {self._url}")

    def push(self, name: str, labels: dict[S, str], samples: dict[date, N]):
        """Buffers `samples` for a timeseries of `name` and `labels` to send as part of the next `flush()`."""

        labelsStr = ",".join((f'{k}="{v}"' for k, v in labels.items()))
        for k, v in sorted(samples.items(), key=lambda t: t[0]):
            self._buffer.append(
                f"{name}{{{labelsStr}}} {round(v, 2)} {int(mktime(k.timetuple()))}"
            )

        print(f"pushed timeseries {name}{labels} with {len(samples)} samples")

    def flush(self):
        with io.BytesIO(("\n".join(self._buffer) + "\n# EOF\n").encode()) as buffer:
            requests.post(f"{self._url}/import", buffer).raise_for_status()

        print(f"flushed {len(self._buffer)} lines to {self._url}")

        self._buffer.clear()
