from typing import Generator, Generic, TypeVar, Generic, Callable, Tuple, Optional, Union, Iterator
from abc import ABC, abstractmethod
from dataclasses import dataclass
from .interfaces import Sink


T = TypeVar('T')
F = TypeVar('F')


def reservoir_sample(source: Union[Iterator[T], Generator[T, None, None]], 
                     sinks: Tuple[Sink[T, F], ...], 
                     randrange: Callable[[int], int],
                     run_length: int) -> None:
    remaining = {}

    for (index, record) in enumerate(source, start=1):
        if randrange(index) == 0:
            chosen = randrange(len(sinks))
            if chosen in remaining:
                sinks[chosen].revert()
            remaining[chosen] = run_length 

        for (key, count) in tuple(remaining.items()):
            sinks[key].consume(record)
            if count == 1:
                del remaining[key]
                sinks[key].commit()
            else:
                remaining[key] = count - 1

    for key in remaining.keys():
        sinks[key].revert()
