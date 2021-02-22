import codecs
import multiprocessing

import tmdbsimple as tmdb

tmdb.API_KEY = ''

from models.Movie import Movie


class MovieScrapper:
    msg_pattern = "<b>MOVIE</b>\nyear: {}, count: {}, errors: {}, pages: {}"

    def __init__(self, years):
        self.years = years
        self.count = 0
        self.errors = 0
        self.pages=0

    def start_thread(self):
        pr = multiprocessing.Process(
            target=self.get_movies_from_api,
            args=(0,)
        )
        pr.start()

    def get_movies_from_api(self, attr):
        search = tmdb.Discover()
        for year in self.years:
            response = search.movie(sort_by="release_date.desc", year=year, page=1)
            array = self.response_to_movies(response)
            self.write_to_csv(array)

            total = response['total_pages']
            self.count = total

            for number in range(1, total+1):
                response = search.movie(sort_by="release_date.desc", year=year, page=number)
                array = self.response_to_movies(response)
                self.write_to_csv(array)

                self.pages+=1

            try:
                from telegram import bot
                bot.send(msg=self.msg_pattern.format(year, self.count, self.errors,self.pages))
            except Exception as e:
                print(e)
            self.count = 0
            self.errors = 0
            self.pages=0

        try:
            from telegram import bot
            bot.send(msg="end MOVIE")
        except Exception as e:
            print(e)

    def response_to_movies(self, response):
        array = []
        for movie in response['results']:
            try:
                array.append(Movie.from_dict(movie))
            except Exception as e:
                self.errors+=1
                print(e)
        return array

    def write_to_csv(self, movies: list):
        with codecs.open("movies.csv", "a+", encoding="UTF-8") as f:
            for movie in movies:
                f.write(str(movie))

    @staticmethod
    def test():
        search = tmdb.Discover()
        response = search.movie(sort_by="release_date.desc", year=2020, page=1)
        movie = Movie.from_dict(response['results'][0])
        print(str(movie))


if __name__ == '__main__':
    # MovieScrapper.test()
    MovieScrapper([range(2010, 2021)]).start_thread()
