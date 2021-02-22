from database.DBService import ShowFilmDB, GenreDB
from keyboards.inline.show_vote_keyboard import show_vote_keyboard
from models.ShowFilm import ShowFilm
from utils import constants, strings
from utils.bot_utils import post_with_image


class ShowCardBot:
    show_id: int
    show: ShowFilm

    def __init__(self, show_id=0):
        self.show_id = show_id

    async def change_state_card(self, bot, callback, state):
        await bot.edit_message_reply_markup(callback.from_user.id, callback.message.message_id,
                                            reply_markup=await show_vote_keyboard(self.show.id, state))

    async def create_random_card(self):
        show = await ShowFilmDB.get_random_show_full()
        genres = await GenreDB.get_genres_by_movie_id(show.id)
        show.change_genres(genres)
        self.show = show
        self.show_id = show.id

    async def set_show(self, show):
        self.show = show

        genres = await GenreDB.get_genres_by_movie_id(show.id)
        self.show.change_genres(genres)

    async def set_show_with_genres(self, show):
        self.show = show
        new_genres = []
        for genre in show.genres:
            new_genres.append(
                (await GenreDB.get_by_pk(genre)).name
            )

    async def create_card(self):
        show = await ShowFilmDB.get_show_by_id_full(self.show_id)
        genres = await GenreDB.get_genres_by_movie_id(show.id)
        show.change_genres(genres)
        self.show = show

    def __create_description(self):
        genres = ""
        for genre in self.show.genres:
            genres += f"#{genre} "
        return strings.film_post_pattern.format(
            constants.show_site_url + f"{self.show.show_type}/{self.show.id}",
            self.show.title,
            genres,
            self.show.original_language,
            self.show.show_type,
            self.show.release_date
        )

    async def send_via_bot(self, client_id, bot):
        await bot.send_photo(client_id,
                             constants.photo_url + self.show.poster,
                             self.__create_description(),
                             reply_markup=await show_vote_keyboard(self.show.id)
                             )
