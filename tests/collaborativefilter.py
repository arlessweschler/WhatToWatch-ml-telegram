import asyncio

import asyncpg
import pandas as pd
from pandas import DataFrame, Series
import numpy as np

from config.DBConfig import DBConfig
from database import DBService
from database.DBService import ConfigurableDB, ShowFilmDB
from scipy import spatial

from tests.newCollabTexstara import CollaborativeFilter


class CollabDB(ConfigurableDB):
    TEST = (
        """ SELECT r1.client_id AS user_id , r1.show_id as film_id
                    ,COUNT(r1.show_id) AS likes_match
                    ,count(*) AS total_match
                    FROM favorites AS r1
                    JOIN favorites AS r2 ON r2.client_id = $1 AND r1.show_id = r2.show_id
                    WHERE r1.client_id <> $1 GROUP BY r1.show_id, r1.client_id
                    ORDER BY r1.client_id limit 10000000;"""
    )
    GET_OWNS_FAVORITES = (
        """SELECT show_id from favorites where client_id = $1"""
    )

    @classmethod
    async def get_test(cls, client_id: int):
        rows = await cls.pool.fetch(cls.TEST, client_id)
        return rows if rows else []

    @classmethod
    async def get_own(cls, client_id: int):
        rows = await cls.pool.fetch(cls.GET_OWNS_FAVORITES, client_id)
        return [row[0] for row in rows] if rows else []


async def prepare():
    pool = await asyncpg.create_pool(user=DBConfig.DB_USER, password=DBConfig.DB_PASSWORD,
                                     database=DBConfig.DB_DATABASE, host=DBConfig.DB_HOST)
    CollabDB.configurate(pool)
    ShowFilmDB.configurate(pool)


async def main():
    await prepare()
    user_ids = []
    film_ids = []
    ratings = []
    # datas = await CollabDB.get_test(54361000)
    datas = await CollabDB.get_test(7000)
    # print(datas)
    for data in datas:
        # print(data[1])
        user_ids.append(
            data[0]
        )
        film_ids.append(
            data[1]
        )
        ratings.append(
            5
        )
    tesqq=[7000]
    tesqq.extend(user_ids)
    user_ids=tesqq
    users =user_ids
    films_=[]
    by_id=[]
    for user in users:
        ttt=await CollabDB.get_own(user)
        films_.extend(
            ttt
        )
        by_id.extend(
            [user for i in ttt]
        )
    data = {
        "userId":by_id,
        "movieId":films_,
        "rating":[5 for i in films_]

    }
    pdd = pd.DataFrame(data=data)
    pdd.to_csv("out.csv",index=False)




    shows_title=[]
    tt=set(films_)
    for ii in tt:
        show = await ShowFilmDB.get_show_by_id(ii)

        shows_title.append(show.title)

    gg=pd.DataFrame(
    data = {
        'movieId':list(tt),
        'title':shows_title
    }
    )
    gg.to_csv("content.csv", index=False)
    # await recommend(df)
    # print(df)
    pass


async def recommend(df: DataFrame):
    # print(df.films.unique())
    original = await test()
    dd: Series = df.groupby(['users'])['films'].apply(list)
    # print(dd)
    qq = dd.sort_values()
    # print(dd)

    dd = qq
    # print(dd)
    # print(type(dd))
    # print(dd.index)
    tt = []

    def check_sum(i: list, orig: list):
        ss = [i[:], orig[:]]
        if len(i) > len(orig):

            for k in range(len(orig), len(i)):
                ss[1].append(0)
        else:
            for k in range(len(i), len(orig)):
                ss[0].append(0)
        return ss

    for k, i in enumerate(dd):
        # print(i)
        # print(original)

        check=check_sum(i,original)
        t = spatial.distance.euclidean(check[0],check[1])
        tt.append(t)

    tt.sort()
    print(tt[3:])

    # dd.nlargest(3,'films')
    # .apply(list)
    # print(dd)
    # print(type(dd))
    #
    # print()
    # print()
    # print()
    # print(dd.head())
    # dq = dd.filter(like='7000', axis=0)
    # print(dq)
    # dq=dd.loc[df['users'] == 54361000]
    # dq=dd.loc[dd['users'].isin([54361000])]
    #
    # print(dq)
    #
    # original = await test()
    # for row in dd:
    #     print(row)
    # spatial.distance.euclidean(original,)
    # break


async def test():
    await DBService.prepare_db()
    collab=CollaborativeFilter(7200)
    data = await collab.get_movies_id_after()
    print(data)

    pass


asyncio.run(test())
# asyncio.run(test())
