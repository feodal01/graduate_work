from collections.abc import Callable, Collection
from dataclasses import dataclass
from datetime import date
from typing import Protocol

FILM_EARLY_ACCESS_READ = 'films.early_access:read'


@dataclass(frozen=True, slots=True)
class FilmAccessContext:
    """Typed factual attributes used by film-access rules."""

    creation_date: date | None = None


class FilmAccessRule(Protocol):
    def required_permissions(
        self,
        context: FilmAccessContext,
    ) -> frozenset[str]: ...


class RecentFilmAccessRule:
    """Require early-access permission for films younger than three years."""

    def __init__(self, *, today: Callable[[], date] = date.today) -> None:
        self._today = today

    def required_permissions(
        self,
        context: FilmAccessContext,
    ) -> frozenset[str]:
        if context.creation_date is None:
            return frozenset({FILM_EARLY_ACCESS_READ})
        threshold = self._calendar_years_ago(self._today(), 3)
        if context.creation_date > threshold:
            return frozenset({FILM_EARLY_ACCESS_READ})
        return frozenset()

    @staticmethod
    def _calendar_years_ago(value: date, years: int) -> date:
        try:
            return value.replace(year=value.year - years)
        except ValueError:
            return value.replace(year=value.year - years, day=28)


class FilmAccessPolicy:
    """Combine the permission requirements of independent film rules."""

    def __init__(self, *, rules: Collection[FilmAccessRule]) -> None:
        self._rules = tuple(rules)

    def required_permissions(
        self,
        context: FilmAccessContext,
    ) -> frozenset[str]:
        permissions: set[str] = set()
        for rule in self._rules:
            permissions.update(rule.required_permissions(context))
        return frozenset(permissions)


def create_film_access_policy() -> FilmAccessPolicy:
    """Create the film policy with the rules enabled by the application."""
    return FilmAccessPolicy(rules=(RecentFilmAccessRule(),))
