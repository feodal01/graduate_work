from dataclasses import dataclass


@dataclass(frozen=True)
class Term:
    field: str
    value: str


@dataclass(frozen=True)
class Match:
    field: str
    value: str
    fuzziness: str | None = None


@dataclass(frozen=True)
class Sort:
    field: str
    descending: bool = False


@dataclass(frozen=True)
class SearchQuery:
    page_number: int
    page_size: int
    match: Match | None = None
    filters: tuple[Term, ...] = ()
    sort: tuple[Sort, ...] = ()
