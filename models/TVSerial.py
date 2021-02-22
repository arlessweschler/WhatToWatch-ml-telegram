from datetime import date
from typing import Optional


class TVSerial:
    id: int
    title: str
    poster: str
    release_date: date
    description: str
    popularity: float
    original_language: Optional['str']
    genres: Optional['list']

    def __init__(self, id, title, poster, release_date, description, popularity, original_language="", genres=None):
        if genres is None:
            genres = []

        self.id = id
        self.title = title
        self.poster = poster
        self.release_date = release_date
        self.description = description
        self.popularity = popularity
        self.original_language = original_language
        self.genres = genres

    def __str__(self):
        genres = ",".join([str(i) for i in self.genres])
        description = self.description.strip().replace("\n", "").replace("\r", "") \
            .replace("\"", " ").replace("\'", "").strip()

        title = self.title.replace("\n", "").replace("\"", " ").replace("\'", " ").replace("\r","").strip()
        return f'{self.id},"{title}",{self.poster},{self.release_date},"{description}",{self.popularity},{self.original_language},"{genres}"\n'

    @staticmethod
    def from_dict(t: dict):
        return TVSerial(
            id=t['id'],
            title=t['name'],
            poster=t['poster_path'],
            release_date=t['first_air_date'],
            description=t['overview'],
            popularity=t['popularity'],
            original_language=t['original_language'],
            genres=t['genre_ids']
        )
