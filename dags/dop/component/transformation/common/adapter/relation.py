from dataclasses import dataclass


class RelationValueError(ValueError):
    pass


@dataclass(frozen=True)
class BaseRelation:
    database: str
    schema: str
    identifier: str
