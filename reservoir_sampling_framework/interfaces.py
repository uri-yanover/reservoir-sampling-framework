from typing import Generator, Generic, TypeVar, Generic, Callable, Tuple, Optional
from abc import ABC, abstractmethod
from dataclasses import dataclass

T = TypeVar('T')
F = TypeVar('F')


class Sink(Generic[T, F], ABC):
    @abstractmethod
    def consume(self, record: T) -> None:
        pass

    @abstractmethod
    def revert(self) -> None:
        pass

    @abstractmethod
    def commit(self) -> None:
        pass

    @abstractmethod
    def finalize(self) -> Optional[F]:
        pass


class Filer(ABC):
    @abstractmethod
    def write(self, data: bytes):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def get_name(self):
        pass

    @abstractmethod
    def cleanup(self):
        pass


@dataclass
class FilerSink(Sink[bytes, str]):
    io_factory: Callable[[], Filer]
    committed: Optional[Filer] = None
    active: Optional[Filer] = None

    def consume(self, record: bytes) -> None:
        if self.active is None:
            self.active = self.io_factory() 
        self.active.write(record)

    def revert(self) -> None:
        if self.active:
            self.active.cleanup()
            self.active = None
        
    def commit(self):
        if self.committed:
            self.committed.cleanup()
        self.active.close()
        self.committed = self.active
        self.active = None
    
    def finalize(self) -> Optional[str]:
        if self.active:
            self.active.cleanup()
        if not self.committed:
            return None
        return self.committed.get_name()


@dataclass
class MultipleSink(Sink[Tuple[T, ...], Tuple[F, ...]]):
    elements: Tuple[Sink[T, F], ...]

    def consume(self, records: Tuple[T, ...]) -> None:
        for (element, record) in zip(self.elements, records):
            element.consume(record)

    def revert(self) -> None:
        for element in self.elements:
            element.revert()
        
    def commit(self):
        for element in self.elements:
            element.commit()
    
    def finalize(self) -> Optional[Tuple[F, ...]]:
        results = [element.finalize() for element in self.elements]
        return tuple(results)

