from datetime import datetime


class ShowFilm:
    id: int
    title: str
    show_type: str
    poster: str
    release_date: datetime
    description: str
    popularity: float
    original_language: int
    genres: list

    def __init__(self, id, title, show_type, poster, release_date,
                 description, popularity, original_language, genres=None):
        if genres is None:
            genres = []

        self.id = id
        self.title = title
        self.show_type = show_type
        self.poster = poster
        self.release_date = release_date
        self.description = description
        self.popularity = popularity
        self.original_language = original_language
        self.genres = genres

    def change_genres(self,genres):
        self.genres=[genre[0] for genre in genres]