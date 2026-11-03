from elasticsearch import AsyncElasticsearch
from pydantic import BaseModel

from db.backoff_policy import on_elastic_exception
from db.health import HealthCheck

from .base import AbstractStorage
from .elastic_errors import run_elastic, with_elastic_errors
from .elastic_query import ElasticQueryTranslator
from .query import SearchQuery


class ElasticStorage[ModelT: BaseModel](AbstractStorage[ModelT], HealthCheck):
    def __init__(
        self,
        index: str,
        es: AsyncElasticsearch,
        model: type[ModelT],
        translator: ElasticQueryTranslator,
    ) -> None:
        self.index = index
        self.es = es
        self.model = model
        self.translator = translator

    @with_elastic_errors
    @on_elastic_exception()
    async def ping(self) -> bool:
        return await self.es.ping()

    @with_elastic_errors
    @on_elastic_exception()
    async def get_by_id(self, item_id: str) -> ModelT | None:
        response = await run_elastic(
            self.es.get(index=self.index, id=item_id),
            missing_document=True,
        )
        if response is None:
            return None
        return self.model.model_validate(response['_source'])

    @with_elastic_errors
    @on_elastic_exception()
    async def search(self, query: SearchQuery) -> list[ModelT]:
        response = await run_elastic(
            self.es.search(
                index=self.index,
                **self.translator.to_body(query),
            ),
        )
        # missing_document=False → None не возвращается
        assert response is not None
        return [self.model.model_validate(hit['_source']) for hit in response['hits']['hits']]
