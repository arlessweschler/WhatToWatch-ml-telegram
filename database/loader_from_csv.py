import codecs
import datetime
import json
from typing import Optional

import asyncpg
import asyncio
from database.DBService import GenreDB, prepare_db, LanguageDB, ShowFilmDB, FilmGenreDB
import pandas as pd

from models.ShowFilm import ShowFilm

s = {
    "genres": [
        {
            "id": 28,
            "name": "Action"
        },
        {
            "id": 12,
            "name": "Adventure"
        },
        {
            "id": 16,
            "name": "Animation"
        },
        {
            "id": 35,
            "name": "Comedy"
        },
        {
            "id": 80,
            "name": "Crime"
        },
        {
            "id": 99,
            "name": "Documentary"
        },
        {
            "id": 18,
            "name": "Drama"
        },
        {
            "id": 10751,
            "name": "Family"
        },
        {
            "id": 14,
            "name": "Fantasy"
        },
        {
            "id": 36,
            "name": "History"
        },
        {
            "id": 27,
            "name": "Horror"
        },
        {
            "id": 10402,
            "name": "Music"
        },
        {
            "id": 9648,
            "name": "Mystery"
        },
        {
            "id": 10749,
            "name": "Romance"
        },
        {
            "id": 878,
            "name": "Science Fiction"
        },
        {
            "id": 10770,
            "name": "TV Movie"
        },
        {
            "id": 53,
            "name": "Thriller"
        },
        {
            "id": 10752,
            "name": "War"
        },
        {
            "id": 37,
            "name": "Western"
        }
    ]
}
ss = {
    "genres": [
        {
            "id": 10759,
            "name": "Action & Adventure"
        },
        {
            "id": 16,
            "name": "Animation"
        },
        {
            "id": 35,
            "name": "Comedy"
        },
        {
            "id": 80,
            "name": "Crime"
        },
        {
            "id": 99,
            "name": "Documentary"
        },
        {
            "id": 18,
            "name": "Drama"
        },
        {
            "id": 10751,
            "name": "Family"
        },
        {
            "id": 10762,
            "name": "Kids"
        },
        {
            "id": 9648,
            "name": "Mystery"
        },
        {
            "id": 10763,
            "name": "News"
        },
        {
            "id": 10764,
            "name": "Reality"
        },
        {
            "id": 10765,
            "name": "Sci-Fi & Fantasy"
        },
        {
            "id": 10766,
            "name": "Soap"
        },
        {
            "id": 10767,
            "name": "Talk"
        },
        {
            "id": 10768,
            "name": "War & Politics"
        },
        {
            "id": 37,
            "name": "Western"
        }
    ]
}


async def load():
    await prepare_db()
    df = pd.read_csv("../data/movies.csv", sep=',', quotechar="\"")
    df = df.fillna("")
    df = df.drop_duplicates(subset=['id'])

    gg = df['original_language'].unique()
    df = df[['id', 'genres']]

    # print(gg)

    df1 = pd.read_csv("../data/serials.csv", sep=',', quotechar="\"")
    gg1 = df1['original_language'].unique()
    df1 = df1.fillna("")
    df1 = df1.drop_duplicates(subset=['id'])

    df1 = df1[['id', 'genres']]
    # print(gg1)
    gg = list(gg)
    gg.extend(gg1)
    gg = set(gg)
    langs = sorted(gg)
    print(len(langs))

    # lang find langs
    for index, row in df.iterrows():
        original = langs.index(row['original_language'])
        tt = row['genres']
        genres = str(tt).replace("\"", "").replace("nan", "").split(",")

        def xoo(genres):
            new_genres = []
            for t in genres:
                if len(t) > 1:
                    new_genres.append(int(t))
                else:
                    pass
            return new_genres

        genres = xoo(genres)
        if row['description'] is None or str(row['description']) == "nan":
            descr = ""
        else:
            descr = row['description']

        if len(row['poster']) < 1 or len(descr) < 1 or row['poster'] is None or len(str(row['poster'])) < 6:
            continue

        if len(row['release_date']) > 1:
            try:
                date = datetime.datetime.strptime(row['release_date'], "%Y-%m-%d")
            except:
                date = None
                pass
        else:
            date = None

        show = ShowFilm(
            id=row['id'],
            title=row['title'],
            show_type=1,
            poster=row['poster'],
            release_date=date,
            description=descr,
            popularity=row['popularity'],
            original_language=original,
            genres=genres
        )
        try:
            await ShowFilmDB.create(show)
        except Exception as e:
            print(show.id)
            print(e)

    pass


# with codecs.open("../data/genres.json") as f:
#     genres = json.load(f)

import csv


async def load1():
    await prepare_db()

    df = pd.read_csv("../data/movies.csv", sep=',', quotechar="\"")
    df = df.fillna("")
    df = df.drop_duplicates(subset=['id'])

    gg = df['original_language'].unique()
    df = df[['id', 'genres']]

    # print(gg)

    df1 = pd.read_csv("../data/serials.csv", sep=',', quotechar="\"")
    gg1 = df1['original_language'].unique()
    df1 = df1.fillna("")
    df1 = df1.drop_duplicates(subset=['id'])

    df1 = df1

    for index, row in df1.iterrows():
        tt = row['genres']
        genres = str(tt).replace("\"", "").replace("nan", "").split(",")

        def xoo(genres):
            new_genres = []
            for t in genres:
                if len(t) > 1:
                    new_genres.append(int(t))
                else:
                    pass
            return new_genres

        genres = xoo(genres)

        try:
            for i in genres:
                await FilmGenreDB.create(row['id'], i)
        except Exception as e:
            print(e)


asyncio.run(load1())

# df = pd.read_csv("../data/movies.csv",sep=', (?=(?:"[^"]*?(?: [^"]*)*))|, (?=[^",]+(?:,|$))',
#             engine='python')
#
#
# def movies_load(df):
#     lsa = df
#     print(lsa)
#
#     pass


# asyncio.run(movies_load(df))

# movies_load(df)


# with codecs.open("../data/movies.csv", 'r', encoding="UTF-8") as f:
#     reader = csv.reader(f, quotechar="\"")
#     next(reader)
#
#     curs = 0
#     for i in reader:
#
#         for k, j in enumerate(i):
#             # print(k, j)
#             if k > 7:
#                 print(curs,i[0])
#                 # print()
#                 # print(k, j)
#                 # print()
#                 # print()
#                 break
#
#         curs += 1
