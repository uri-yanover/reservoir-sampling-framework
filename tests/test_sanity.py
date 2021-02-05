from sys import path
import os

path.append(os.path.join(os.path.dirname(__file__), '..'))

from dataclasses import dataclass, field
from functools import partial
from typing import List

from reservoir_sampling_framework import Filer, reservoir_sample, FilerSink
from random import randrange, seed

@dataclass
class InMemoryFiler(Filer):
    name: str
    is_closed: bool = False
    content: List[bytes] = field(default_factory=list)

    def write(self, data: bytes):
        if self.is_closed:
            raise ValueError('Cannot write while closed')
        self.content.append(data)

    def close(self):
        self.is_closed = True
        print('Closing up', self)

    def get_name(self):
        return self.name

    def cleanup(self):
        self.is_closed = True
        print('Cleaning up', self)


_PREFIXES = {}

def in_memory_filer_factory(prefix='x') -> Filer:
    index = _PREFIXES.get(prefix, 0)
    _PREFIXES[prefix] = index + 1
    return InMemoryFiler(f'filer {prefix} {index}')


def test_single_sink():
    seed(42)
    sink = FilerSink(in_memory_filer_factory)
    source = iter((str(k).encode('utf-8') for k in range(1000)))

    reservoir_sample(source=source, 
                     sinks=(sink,), 
                     randrange=randrange,
                     run_length=15)


def test_double_sink():
    seed(43)
    print('Z')
    sink1 = FilerSink(partial(in_memory_filer_factory, 's1'))
    sink2 = FilerSink(partial(in_memory_filer_factory, 's2'))

    source = iter((str(k).encode('utf-8') for k in range(100000)))

    reservoir_sample(source=source, 
                     sinks=(sink1, sink2,), 
                     randrange=randrange,
                     run_length=15)


if __name__ == '__main__':
    test_double_sink()
