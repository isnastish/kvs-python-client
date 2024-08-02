import typing as t
from dataclasses import dataclass, field
from yarl import URL

import numpy as np


@dataclass
class BaseResult:
    """Base result for all kvs commands."""
    status: int = field(default=0)
    url: URL = field(default=URL())
    error: t.Optional[str] = None
    params: t.Optional[tuple[t.Any]] = None


@dataclass
class StrResult(BaseResult):
    """Result for kvs commands returning a string."""
    result: str = field(default="")


@dataclass
class IntResult(BaseResult):
    """Result for kvs commands returning an integer."""
    result: int = field(default=0)


@dataclass
class BoolResult(BaseResult):
    """Result for kvs commands returning bool."""
    result: bool = field(default=False)


@dataclass
class FloatResult(BaseResult):
    """Result for kvs commands returning float."""
    result: float = field(default=0.0)


@dataclass 
class DictResult(BaseResult):
    """Result for kvs commands returning dict."""
    # NOTE: Default value for dict won't work here, we have to use default_factory instead, 
    # see: https://stackoverflow.com/questions/53632152/why-cant-dataclasses-have-mutable-defaults-in-their-class-attributes-declaratio
    result: dict[str, str] = field(default_factory=dict)


@dataclass
class UintResult(BaseResult):
    """Uint kvs command result"""
    result: np.uint32 = 0