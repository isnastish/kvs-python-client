import typing as t
from dataclasses import dataclass, field
from yarl import URL

@dataclass
class BaseResult:
    """_summary_
    """
    
    status: int = field(default=0)
    url: URL = field(default=URL())
    error: t.Optional[str] = None
    params: t.Optional[tuple[t.Any]] = None


@dataclass
class StrResult(BaseResult):
    """
    """
    result: str = field(default="")


@dataclass
class IntResult(BaseResult):
    """_summary_
    """
    result: int = field(default=0)


@dataclass
class BoolResult(BaseResult):
    """_summary_
    """
    result: bool = field(default=False)


@dataclass
class FloatResult(BaseResult):
    """_summary_
    """
    result: float = field(default=0.0)


@dataclass 
class DictResult(BaseResult):
    """_summary_
    """
    # NOTE: Default value for dict won't work here, we have to use default_factory instead, 
    # see: https://stackoverflow.com/questions/53632152/why-cant-dataclasses-have-mutable-defaults-in-their-class-attributes-declaratio
    result: dict[str, str] = field(default_factory=dict)
