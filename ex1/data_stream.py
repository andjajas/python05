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


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for elem in stream:
            any_process = False
            for proc in self.processors:
                if proc.validate(elem):
                    proc.ingest(elem)
                    any_process = True
            if not any_process:
                print(
                    "DataStream error - Can't process element in"
                    f" stream: {elem}"
                )

    @staticmethod
    def add_space_before_cap(name: str) -> str:
        spaced_name = ""
        for i in range(len(name)):
            if name[i].isupper() and i != 0:
                spaced_name += " "
            spaced_name += name[i]
        return spaced_name

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
        else:
            for proc in self.processors:
                name = type(proc).__name__
                spaced_name = self.add_space_before_cap(name)
                print(f"{spaced_name}: total {proc.counter} items "
                      f"processed, remaining {len(proc.queue)} on processor")


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===\n")
    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()
    num_proc = NumericProcessor()
    print("\nRegistering Numeric Processor\n")
    stream.register_processor(num_proc)
    batch: list[Any] = [
        'Hello world',
        [3.14, -1, 2.71],
        [
            {
                'log_level': 'WARNING',
                'log_message': 'Telnet access! Use ssh instead'
            },
            {'log_level': 'INFO', 'log_message': 'User wil is connected'}
        ],
        42,
        ['Hi', 'five']
        ]
    print(f"Send first batch of data on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors_stats()
    print("\nRegistering other data processors")
    txt_proc = TextProcessor()
    stream.register_processor(txt_proc)
    log_proc = LogProcessor()
    stream.register_processor(log_proc)
    print("Send the same batch again")
    stream.process_stream(batch)
    stream.print_processors_stats()
    for i in range(3):
        num_proc.output()
    for i in range(2):
        txt_proc.output()
    log_proc.output()
    print(
        "\nConsume more elements from the data processors: Numeric 3, "
        "Text 2, Log 1"
     )
    stream.print_processors_stats()
