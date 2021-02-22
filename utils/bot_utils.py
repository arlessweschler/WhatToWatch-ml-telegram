from aiogram import types

from models.ShowFilm import ShowFilm
from utils import strings, constants


def post_with_image(movie: ShowFilm):
    genres = ""
    for genre in movie.genres:
        genres += f"#{genre} "
    #     constants.photo_url + movie.poster,
    return strings.film_post_pattern.format(
        constants.show_site_url + f"{movie.show_type}/{movie.id}",
        movie.title,
        genres,
        movie.original_language,
        movie.show_type,
        movie.release_date
    )


def parse_dict_to_showfilm(data: dict):
    return ShowFilm(
        id=data['id'],
        title=data['title'],
        description=data['overview'],
        original_language=data['original_language'],
        popularity=data['popularity'],
        poster=data['poster_path'],
        release_date=data['release_date'],
        show_type=1,
        genres=data['genre_ids']
    )

#     https://www.themoviedb.org/
