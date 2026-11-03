from pymongo.asynchronous.client_session import AsyncClientSession

from events.contracts import LikesSnapshot
from models.documents import LikesOutbox, utc_now


async def append_likes_event(session: AsyncClientSession, snapshot: LikesSnapshot) -> None:
    await LikesOutbox.get_pymongo_collection().insert_one(
        {**snapshot.model_dump(), 'created_at': utc_now(), 'published_at': None},
        session=session,
    )
