from datetime import date

from pydantic import BaseModel


class Person(BaseModel):
    id: str
    name: str


class Genre(BaseModel):
    id: str
    name: str


class Film(BaseModel):
    id: str
    creation_date: date | None = None
    imdb_rating: float | None = None
    genres: list[Genre]
    title: str
    description: str | None = None
    directors_names: list[str]
    actors_names: list[str]
    writers_names: list[str]
    directors: list[Person]
    actors: list[Person]
    writers: list[Person]
