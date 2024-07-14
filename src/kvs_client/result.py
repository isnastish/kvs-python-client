import typing as t
from dataclasses import dataclass, field
from yarl import URL

# NOTE: Use inheritance instead to access status/url/error members directly

@dataclass
class BaseResult:
    """Base result for all kvs commands."""
    status: int = field(default=0)
    url: URL = field(default=URL())
    error: t.Optional[str] = None
    params: t.Optional[tuple[t.Any]] = None


@dataclass
class StrResult:
    """Result for kvs commands returning a string."""
    base: BaseResult = field(default_factory=BaseResult())
    result: str = field(default="")


@dataclass
class IntResult:
    """Result for kvs commands returning an integer."""
    base: BaseResult = field(default_factory=BaseResult())
    result: int = field(default=0)


@dataclass
class BoolResult:
    """Result for kvs commands returning bool."""
    base: BaseResult = field(default_factory=BaseResult())
    result: bool = field(default=False)


@dataclass
class FloatResult:
    """Result for kvs commands returning float."""
    base: BaseResult = field(default_factory=BaseResult())
    result: float = field(default=0.0)


@dataclass 
class DictResult(BaseResult):
    """Result for kvs commands returning dict."""
    # NOTE: Default value for dict won't work here, we have to use default_factory instead, 
    # see: https://stackoverflow.com/questions/53632152/why-cant-dataclasses-have-mutable-defaults-in-their-class-attributes-declaratio
    base: BaseResult = field(default_factory=BaseResult())
    result: dict[str, str] = field(default_factory=dict)
