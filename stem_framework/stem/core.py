from typing import Optional
from typing_extensions import Protocol
import dataclasses
def pascal_case_to_snake_case(name: str) -> str:
    ... # TODO()



class Named:
    _name: Optional[str] = None

    @property
    def name(self):
        ... # TODO()


class Dataclass(Protocol):
    a: int=0
    b: float=1.0
    c: list = dataclasses.field(default_factory=list)
    name=Optional[str]='dict'
    