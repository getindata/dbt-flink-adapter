from dataclasses import dataclass, field
from dbt.adapters.base.relation import BaseRelation, Policy


@dataclass
class FlinkQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class FlinkIncludePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class FlinkRelation(BaseRelation):
    include_policy: FlinkIncludePolicy = field(default_factory=FlinkIncludePolicy)
    quote_policy: FlinkQuotePolicy = field(default_factory=FlinkQuotePolicy)
