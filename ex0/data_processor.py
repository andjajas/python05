#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__ (self) -> None:
        self.queue = []
        self.counter = 0
    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass
    
    def output(self) -> tuple[int, str]:
        pass


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        pass

    def ingest(
        self,
        num_data: int | float | list[int | float]
         ) -> None:
        if isinstance(num_data, )


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        pass

    def ingest(self):
        pass


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        pass

    def ingest(self):
        pass


# the ingest methods in the specialized classes will have their own
# specific signatures to match the types they expect.
# In case the user does not validate the data before calling ingest,
# and provides invalid data, an exception must be raised.
