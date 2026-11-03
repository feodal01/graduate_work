"""Транзакционный барьер для будущего исходного снимка рецензий.

256 постоянных документов review_write_gate с целыми _id 0..255. Bucket =
int.from_bytes(ObjectId.binary, 'big') % 256. Создание рецензии выбирает ObjectId
до входа в транзакцию. Каждая запись увеличивает revision своего bucket при
frozen=False. Будущий bootstrap должен в одной транзакции установить frozen=True
во всех 256 документах и дождаться commit перед чтением снимка. Конфликты записей
не дают ему завершиться до завершения ранее начатых изменений. Проверка одного
флага без записи такого барьера не создаёт. Существующие флаги lifespan не сбрасывает.
"""

from beanie import PydanticObjectId
from pymongo import UpdateOne
from pymongo.asynchronous.client_session import AsyncClientSession

from core.config import settings
from dependencies import get_mongo_client
from services.errors import WritesPausedError

REVIEW_WRITE_BUCKETS = 256
REVIEW_WRITE_GATE_COLLECTION = 'review_write_gate'


async def initialize_review_write_gate() -> None:
    await get_mongo_client()[settings.mongo_db][REVIEW_WRITE_GATE_COLLECTION].bulk_write(
        [
            UpdateOne(
                {'_id': bucket},
                {'$setOnInsert': {'frozen': False, 'revision': 0}},
                upsert=True,
            )
            for bucket in range(REVIEW_WRITE_BUCKETS)
        ]
    )


async def acquire_review_write(session: AsyncClientSession, review_id: PydanticObjectId) -> None:
    bucket = int.from_bytes(review_id.binary, 'big') % REVIEW_WRITE_BUCKETS
    result = await get_mongo_client()[settings.mongo_db][REVIEW_WRITE_GATE_COLLECTION].update_one(
        {'_id': bucket, 'frozen': False}, {'$inc': {'revision': 1}}, session=session
    )
    if not result.matched_count:
        raise WritesPausedError
