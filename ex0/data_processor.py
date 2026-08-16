#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
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
                raise TypeError("Improper numeric data")


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
                raise TypeError("Improper text data")


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if (isinstance(data, dict)
                and all(isinstance(elem, str) for elem in data.keys())
                and all(isinstance(elem, str) for elem in data.values())):
            return True
        elif isinstance(data, list):
            return all(self.validate(elem) for elem in data)
        else:
            return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if isinstance(data, list):
            data_list = data
        else:
            data_list = [data]
        for elem in data_list:
            if self.validate(elem):
                elem_str = ": ".join(elem.values())
                elem_tup = (self.counter, elem_str)
                self.queue.append(elem_tup)
                self.counter += 1
            else:
                raise TypeError("Improper dict data")


if __name__ == "__main__":
    num_proc = NumericProcessor()
    txt_proc = TextProcessor()
    log_proc = LogProcessor()
    print("=== Code Nexus - Data Processor ===")
    print("\nTesting Numeric Processor...")
    print(f" Trying to validate input '42': {num_proc.validate(42)}")
    print(f" Trying to validate input 'Hello': {num_proc.validate('Hello')}")
    try:
        num_proc.ingest("some invalid string")
    except TypeError as e:
        print(f" Got exception: {e}")
    data1: list[int | float] = [1, 2, 3, 4, 5]
    print(f" Processing data: {data1}")
    num_proc.ingest(data1)
    n = 3
    print(f" Extracting {n} values")
    for i in range(n):
        rank, value = num_proc.output()
        print(f" Numeric value {rank}: {value}")

    print("\nTesting Text Processor...")
    print(f" Trying to validate input '42': {txt_proc.validate(42)}")
    print(f" Trying to validate input 'Hello': {txt_proc.validate('Hello')}")
    try:
        txt_proc.ingest([11, 33, 55])
    except TypeError as e:
        print(f" Got exception: {e}")
    data2 = ['Hello', 'Nexus', 'World']
    print(f" Processing data: {data2}")
    txt_proc.ingest(data2)
    n = 1
    print(f" Extracting {n} value...")
    for i in range(n):
        rank, value = txt_proc.output()
        print(f" Text value {rank}: {value}")

    print("\nTesting Log Processor...")
    print(f" Trying to validate input '42': {log_proc.validate(42)}")
    print(f" Trying to validate input 'Hello': {log_proc.validate('Hello')}")
    sample = {'Hello': 'How are you?'}
    print(f" Trying to validate input {sample}: {log_proc.validate(sample)}")
    try:
        log_proc.ingest([11, 33, 55])
    except TypeError as e:
        print(f" Got exception: {e}")
    data3 = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'},
    ]
    print(f" Processing data: {data3}")
    log_proc.ingest(data3)
    n = 2
    print(f" Extracting {n} values...")
    for i in range(n):
        rank, value = log_proc.output()
        print(f" Log entry {rank}: {value}")
