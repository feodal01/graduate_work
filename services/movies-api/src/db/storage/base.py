from abc import ABC, abstractmethod

from pydantic import BaseModel

from .query import SearchQuery


class AbstractStorage[ModelT: BaseModel](ABC):
    @abstractmethod
    async def get_by_id(self, item_id: str) -> ModelT | None: ...

    @abstractmethod
    async def search(self, query: SearchQuery) -> list[ModelT]: ...
