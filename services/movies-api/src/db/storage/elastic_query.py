from typing import Any

from .query import SearchQuery


class ElasticQueryTranslator:
    def __init__(self, nested_paths: frozenset[str] = frozenset()) -> None:
        self.nested_paths = nested_paths

    def to_body(self, query: SearchQuery) -> dict[str, Any]:
        body: dict[str, Any] = {
            'from_': (query.page_number - 1) * query.page_size,
            'size': query.page_size,
            'query': self._build_query(query),
        }
        if query.sort:
            body['sort'] = [
                {
                    sort.field: {
                        'order': 'desc' if sort.descending else 'asc',
                        'missing': '_last',
                    }
                }
                for sort in query.sort
            ]
        return body

    def _build_query(self, query: SearchQuery) -> dict[str, Any]:
        conditions: dict[str, Any] = {}

        if query.match is not None:
            field = query.match.field
            options: dict[str, Any] = {'query': query.match.value}
            if query.match.fuzziness is not None:
                options['fuzziness'] = query.match.fuzziness
            conditions['must'] = [self._nested(field, {'match': {field: options}})]

        if query.filters:
            conditions['filter'] = [
                self._nested(term.field, {'term': {term.field: term.value}})
                for term in query.filters
            ]

        return {'bool': conditions} if conditions else {'match_all': {}}

    def _nested(self, field: str, condition: dict[str, Any]) -> dict[str, Any]:
        path = field.partition('.')[0]
        if path not in self.nested_paths:
            return condition
        return {'nested': {'path': path, 'query': condition}}
