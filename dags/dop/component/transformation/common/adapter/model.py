from dataclasses import dataclass


@dataclass(frozen=True)
class Argument:
    name: str
    type: str
