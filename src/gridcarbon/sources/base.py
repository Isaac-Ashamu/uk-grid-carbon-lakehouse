"""Source abstraction: something that, given a partition, produces raw fetches."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Protocol

from gridcarbon.domain import Partition
from gridcarbon.http import FetchResult, HttpClient


@dataclass(frozen=True)
class RawFetch:
    """One HTTP response plus the provenance bronze needs to store it faithfully."""

    source: str
    dataset: str
    partition: Partition
    result: FetchResult
    request: dict[str, Any] = field(default_factory=dict)


class Source(Protocol):
    name: str
    datasets: tuple[str, ...]

    def fetch(self, client: HttpClient, partition: Partition, dataset: str) -> Iterator[RawFetch]:
        """Yield one or more raw fetches for ``dataset`` in ``partition``."""
        ...
