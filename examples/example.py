#!/usr/bin/env python3
from dataclasses import dataclass, field
from reservoir_sampling_framework import FilerSink, SinkUsageIntent
from typing import BinaryIO, List
from os import remove

from random import randrange, seed

from reservoir_sampling_framework import Filer, reservoir_sample

@dataclass
class FileFiler(Filer):
    name: str
    file_object: BinaryIO

    def write(self, data: bytes):
        if self.file_object.closed:
            raise ValueError('Cannot write while closed')
        self.file_object.write(data)

    def close(self):
        if not self.file_object.closed:
            self.file_object.close()

    def get_name(self):
        return self.name

    def cleanup(self):
        self.close()
        remove(self.name)


def run(source_file_name: str, destination_stem: str, destination_count: int, run_length: int):
    counter = [0]
    def in_memory_filer_factory() -> Filer:
        file_name = f'{destination_stem}_{counter[0]}'
        file_object = open(file_name, 'wb')
        counter[0] += 1
        return FileFiler(file_name, file_object)
    
    sinks = tuple(SinkUsageIntent(FilerSink(in_memory_filer_factory), run_length) for _ in range(destination_count))

    with open(source_file_name, 'rb') as source_file_object:
        iterator = iter(source_file_object)
        reservoir_sample(iterator, 
                     sink_usage_intents=sinks, 
                     randrange=randrange) 

    return [sink.sink.finalize() for sink in sinks]


if __name__ == '__main__':
    from sys import argv

    seed(42)
    print(run(argv[1], argv[2], int(argv[3]), int(argv[4])))