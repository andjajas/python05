#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__ (self) -> None:
        self.queue: list[tuple[int, str]] = []
        self.counter: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        return self.queue.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list):
            return all(isinstance(elem, (int, float)) for elem in data)
        else:
            return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if isinstance(data, list):
            data_list = data
        else:
            data_list = [data]
        for elem in data_list:
            if self.validate(elem):
                elem_str = str(elem)
                elem_tup = (self.counter, elem_str)
                self.queue.append(elem_tup)
                self.counter += 1
            else:
                raise TypeError("Got exception: Improper numeric data")


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list):
            return all(isinstance(elem, str) for elem in data)
        else:
            return False

    def ingest(self, data: str | list[str]) -> None:
        if isinstance(data, list):
            data_list = data
        else:
            data_list = [data]
        for elem in data_list:
            if self.validate(elem):
                elem_tup = (self.counter, elem)
                self.queue.append(elem_tup)
                self.counter += 1
            else:
                raise TypeError("Got exception: Improper text data")


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        pass

    def ingest(self):
        pass


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")
    print("\nTesting Numeric Processor...")
# the ingest methods in the specialized classes will have their own
# specific signatures to match the types they expect.
# In case the user does not validate the data before calling ingest,
# and provides invalid data, an exception must be raised.
# use assert of isinstance