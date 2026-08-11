#!/usr/bin/env python3
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    @abstractmethod
    def validate(self):
        pass

    @abstractmethod
    def ingest(self):
        pass
    
    def output(self):
        pass


class NumericProcessor(DataProcessor):
    def validate(self):
        pass

    def ingest(self):
        pass


class TextProcessor(DataProcessor):
    def validate(self):
        pass

    def ingest(self):
        pass


class LogProcessor(DataProcessor):
    def validate(self):
        pass

    def ingest(self):
        pass
