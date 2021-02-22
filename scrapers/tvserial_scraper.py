import codecs
import multiprocessing

import tmdbsimple as tmdb

tmdb.API_KEY = ''

from models.TVSerial import TVSerial


class TVSerialScraper:
    msg_pattern = "<b>TV</b>\nyear: {}, count: {}, errors: {}, pages: {}"

    def __init__(self, years):
        self.years = years
        self.count = 0
        self.errors = 0
        self.page=0

    def start_thread(self):
        pr = multiprocessing.Process(
            target=self.get_serials_from_api,
            args=(0,)
        )
        pr.start()

    def get_serials_from_api(self, attr):
        search = tmdb.Discover()
        for year in self.years:
            response = search.tv(sort_by="release_date.desc", first_air_date_year=year, page=1)
            array = self.response_to_serials(response)
            self.write_to_csv(array)

            total = response['total_pages']
            self.count = total
            for number in range(1, total+1):
                response = search.tv(sort_by="release_date.desc", first_air_date_year=year, page=number)
                array = self.response_to_serials(response)
                self.write_to_csv(array)

                self.page+=1

            try:
                from telegram import bot
                bot.send(msg=self.msg_pattern.format(year, self.count, self.errors,self.page))
            except Exception as e:
                print(e)
            self.count = 0
            self.errors = 0
            self.page=0
        try:
            from telegram import bot
            bot.send(msg="end TV")
        except Exception as e:
            print(e)

    def response_to_serials(self, response):
        array = []
        for serial in response['results']:
            try:
                array.append(TVSerial.from_dict(serial))
            except Exception as e:
                self.errors += 1
                print(e)
        return array

    def write_to_csv(self, serials: list):
        with codecs.open("serials.csv", "a+", encoding="UTF-8") as f:
            for serial in serials:
                f.write(str(serial))

    @staticmethod
    def test():
        search = tmdb.Discover()
        response = search.tv(sort_by="release_date.desc", first_air_date_year=2020, page=1)
        serial = TVSerial.from_dict(response['results'][0])
        print(str(serial))


if __name__ == '__main__':
    # TVSerialScraper.test()
    # TVSerialScraper([range(2010, 2021)]).start_thread()
    TVSerialScraper([2020]).start_thread()
