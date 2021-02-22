import tmdbsimple as tmdb

tmdb.API_KEY = ''

# search = tmdb.Discover()
# genres= tmdb.Genres
# print(genres.movie_list())
# print(genres.tv_list())

if __name__ == '__main__':
    from telegram import bot
    bot.send("started")
    from scrapers import movie_scraper, tvserial_scraper
    years=[i for i in range(2010,2021)]
    tvserial_scraper.TVSerialScraper(years).start_thread()
    movie_scraper.MovieScrapper(years).start_thread()
