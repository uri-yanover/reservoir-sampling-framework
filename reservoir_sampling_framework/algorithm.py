from typing import Generator, TypeVar, Generic, Callable, Tuple, Optional, Union, Iterator
from dataclasses import dataclass
from logging import getLogger

from .interfaces import Sink


_LOGGER = getLogger(__name__)
_DEFAULT_LOGGING_PERIOD = 1 << 20

T = TypeVar('T')
F = TypeVar('F')


@dataclass
class SinkUsageIntent(Generic[T, F]):
    sink: Sink[T, F]
    run_length: int


def reservoir_sample(source: Union[Iterator[T], Generator[T, None, None]], 
                     sink_usage_intents: Tuple[SinkUsageIntent[T, F], ...], 
                     randrange: Callable[[int], int],
                     logging_period=_DEFAULT_LOGGING_PERIOD) -> None:
    remaining = {}

    for (index, record) in enumerate(source, start=1):
        if index % logging_period == 0:
            _LOGGER.info(f'At index {index}. Size of remaining {len(remaining)}')

        if randrange(index) < len(sink_usage_intents):
            chosen = randrange(len(sink_usage_intents))
            if chosen in remaining:
                sink_usage_intents[chosen].sink.revert()
            remaining[chosen] = sink_usage_intents[chosen].run_length

        for (key, count) in tuple(remaining.items()):
            sink_usage_intents[key].sink.consume(record)
            if count == 1:
                del remaining[key]
                sink_usage_intents[key].sink.commit()
            else:
                remaining[key] = count - 1

    _LOGGER.info('Wrapping up')
    for key in remaining.keys():
        sink_usage_intents[key].sink.revert()
