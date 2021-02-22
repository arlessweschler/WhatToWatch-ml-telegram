import asyncio
from typing import Optional

import pandas as pd
from pandas import DataFrame

from database.DBService import prepare_db, ShowFilmDB, ClientDB, FavoriteDB
from models.ShowFilm import ShowFilm

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


async def loader():
    await prepare_db()
    df = pd.read_csv('movies.dat', sep='::', engine='python')
    df = df.sort_values(by=['year'], ascending=False)
    dfrating = pd.read_csv('ratings.dat', sep='::', engine='python')

    print(df.head())

    for index, row in df.iterrows():
        title = row['title']
        try:
            film: Optional[ShowFilm] = await ShowFilmDB.get_show_by_name(title)
            a = film.id

            print("---------------------------------------------------")
            print(film.id, film.title)
            # print(dfrating.loc[dfrating['film']] == row['index'])
            gg = dfrating.loc[dfrating['film'].isin([row['index']])]
            for idn, rw in gg.iterrows():
                try:
                    await ClientDB.create_custom(rw['user_id'] * 1000)
                except Exception as e:
                    print(e)
                try:
                    await FavoriteDB.create(rw['user_id'] * 1000, film.id)
                except Exception as e:
                    print(e)

            print(gg)
            print("---------------------------------------------------")

        except AttributeError as e:
            pass
        except Exception as e:
            print(e)

            pass


asyncio.run(loader())

# print(dfrating.head())
