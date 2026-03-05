"""
LSEG Toolkit - Financial data extraction and reporting tools using LSEG API.

Core utilities for accessing LSEG/Refinitiv data and generating reports.
"""

from .client import LsegClient
from .data import DataProcessor
from .excel import ExcelExporter
from .exceptions import (
    ConfigurationError,
    DataRetrievalError,
    DataValidationError,
    LsegError,
    SessionError,
)

__version__ = "0.1.0"
__all__ = [
    "LsegClient",
    "DataProcessor",
    "ExcelExporter",
    "LsegError",
    "SessionError",
    "DataRetrievalError",
    "DataValidationError",
    "ConfigurationError",
]
