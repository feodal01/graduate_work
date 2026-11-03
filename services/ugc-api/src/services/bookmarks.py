from uuid import UUID

from pymongo import ReturnDocument

from models.documents import Bookmark, utc_now


async def add_bookmark(user_id: UUID, film_id: UUID) -> Bookmark:
    """Идемпотентно добавляет закладку: повтор возвращает существующую."""
    document = await Bookmark.get_pymongo_collection().find_one_and_update(
        {'user_id': user_id, 'film_id': film_id},
        {'$setOnInsert': {'created_at': utc_now()}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    bookmark: Bookmark = Bookmark.model_validate(document)
    return bookmark


async def delete_bookmark(user_id: UUID, film_id: UUID) -> bool:
    result = await Bookmark.get_pymongo_collection().delete_one(
        {'user_id': user_id, 'film_id': film_id},
    )
    return bool(result.deleted_count)


async def list_bookmarks(user_id: UUID, *, skip: int, limit: int) -> list[Bookmark]:
    return (
        await Bookmark.find(Bookmark.user_id == user_id)
        .sort('-created_at')
        .skip(skip)
        .limit(limit)
        .to_list()
    )
