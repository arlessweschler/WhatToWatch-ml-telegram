from datetime import datetime
from typing import Optional, List

import asyncpg
from abc import ABC
from config.DBConfig import DBConfig
from models.Client import Client
from models.Genre import Genre
from models.Languages import Languages
from models.ShowFilm import ShowFilm


class ConfigurableDB(ABC):
    pool: asyncpg.pool.Pool

    @classmethod
    def configurate(cls, pool: asyncpg.pool.Pool):
        cls.pool = pool


class ClientDB(ConfigurableDB):
    CREATE_TABLE = (
        """create table if not exists client
        (
            id integer not null constraint client_pk primary key,
            current_state integer default 0,
            isreal boolean default true,
            created_at timestamp default current_timestamp()
        );
        """
    )
    CREATE_CLIENT = (
        "insert into client (id) values($1)"
    )
    CREATE_CUSTOM_CLIENT = (
        "insert into client (id, isreal) values ($1,$2)"
    )

    SELECT_BY_ID = (
        "select id, current_state, created_at from client where id = $1"
    )
    UPDATE_BY_ID = (
        "update client set current_state = cast($2 as integer ) where id = $1"
    )

    @classmethod
    async def create_table(cls):
        await cls.pool.execute(cls.CREATE_TABLE)

    @classmethod
    async def create(cls, pk: int):
        await cls.pool.execute(cls.CREATE_CLIENT, pk)

    @classmethod
    async def create_custom(cls, pk: int):
        await cls.pool.execute(cls.CREATE_CUSTOM_CLIENT, pk, False)

    @classmethod
    async def get_by_pk(cls, client_pk: int) -> Optional["Client"]:
        row = await cls.pool.fetch(cls.SELECT_BY_ID, client_pk)
        print(row)
        return Client(*(row[0])) if row else None

    @classmethod
    async def update(cls, client: "Client"):
        await cls.pool.execute(cls.UPDATE_BY_ID, client.pk, client.current_state)


class ShowFilmDB(ConfigurableDB):
    CREATE_TABLE = (
        """
        create table if not exists showfilm
        id int8 NOT NULL,
        title varchar(100),
        show_type int4,
        poster varchar(255),
        release_date date,
        description text,
        popularity decimal(7,4),
        original_language int4,
        CONSTRAINT showfilm_languages_id_fk FOREIGN KEY (original_language) REFERENCES languages(id),
        CONSTRAINT showfilm_show_type_id_fk FOREIGN KEY (show_type) REFERENCES show_type(id),
        PRIMARY KEY (id)
    """
    )
    CREATE_ROW = (
        """ INSERT INTO showfilm ("id", "title", "show_type", "poster", 
            "release_date", "description", "popularity","original_language")
            VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8);"""
    )
    GET_SHOW = (
        """ SELECT sm.id,title,st.name as show_type,poster,release_date,description,popularity,lg.name as original_language 
            from showfilm sm  left join languages lg on sm.original_language = lg.id 
            left join "show_type" st on sm.show_type=st.id
            WHERE release_date<=$1  order by release_date desc limit 1;
          """
    )
    GET_SHOW_BY_NAME = (
        """SELECT sm.id,title,st.name as show_type,poster,release_date,description,popularity,lg.name as original_language 
            from showfilm sm  left join languages lg on sm.original_language = lg.id 
            left join "show_type" st on sm.show_type=st.id
            WHERE title=$1  order by release_date desc limit 1;"""
    )

    GET_SHOWS = (
        """ SELECT sm.id,title,st.name as show_type,poster,release_date,description,popularity,lg.name as original_language 
            from showfilm sm  left join languages lg on sm.original_language = lg.id 
            left join "show_type" st on sm.show_type=st.id
            WHERE release_date<=$1  order by popularity desc limit $2;
          """
    )
    GET_SHOW_BY_ID = (
        """SELECT sm.id,title,st.name as show_type,poster,release_date,description,popularity,lg.name as original_language 
            from showfilm sm  left join languages lg on sm.original_language = lg.id 
            left join "show_type" st on sm.show_type=st.id
            WHERE sm.id = $1 """
    )
    GET_SHOW_BY_ID_FULL = (
        """SELECT sm.id,title,st.name as show_type,poster,release_date,description,popularity,lg.name as original_language 
            from showfilm sm  left join languages lg on sm.original_language = lg.id 
            left join "show_type" st on sm.show_type=st.id
            WHERE sm.id = $1 """
    )

    GET_SHOWS_FILTER_DATE = (
        """ SELECT sm.id,title,st.name as show_type,poster,release_date,description,popularity,lg.name as original_language 
            from showfilm sm  left join languages lg on sm.original_language = lg.id 
            left join "show_type" st on sm.show_type=st.id
            WHERE release_date between $1 and $2 order by popularity desc limit $3;
          """
    )
    GET_RANDOM_FILM = (
        """SELECT sm.id,title,st.name as show_type,poster,release_date,description,popularity,lg.name as original_language 
            from showfilm sm  left join languages lg on sm.original_language = lg.id 
            left join "show_type" st on sm.show_type=st.id
            ORDER by RANDOM() limit 1"""
    )

    GET_RECOMMENDED_FILM_BY_CLIENTS = (
        """ SELECT  show_id FROM favorites JOIN showfilm sf on show_id = id 
            WHERE sf.release_date >= $1 GROUP by show_id order by COUNT(show_id) DESC limit $2 offset $3"""
    )
    GET_POPULAR_FILM_BY_DATE = (
        """SELECT sm.id,title,st.name as show_type,poster,release_date,description,popularity,lg.name as original_language 
            from showfilm sm  left join languages lg on sm.original_language = lg.id 
            left join "show_type" st on sm.show_type=st.id
            WHERE release_date >= $1 order by popularity desc limit 10"""
    )
    GET_SHOWS_WATCHING_NOW = (
        """SELECT  show_id FROM favorites JOIN showfilm sf on show_id = id 
            WHERE sf.release_date >= $1 GROUP by show_id order by COUNT(show_id) limit 5 """
    )

    # ORDER BY RAND()
    # LIMIT 1

    # '2020-12-31 00:00:00.000'

    @classmethod
    async def create_table(cls):
        await cls.pool.execute(cls.CREATE_TABLE)

    @classmethod
    async def create(cls, show: ShowFilm):
        await cls.pool.execute(cls.CREATE_ROW, show.id, show.title, show.show_type, show.poster, show.release_date,
                               show.description, show.popularity, show.original_language)

    @classmethod
    async def get_show(cls, date: datetime) -> Optional["ShowFilm"]:
        row = await cls.pool.fetch(cls.GET_SHOW, date)
        return ShowFilm(*(row[0])) if row else None

    @classmethod
    async def get_show_by_name(cls, title: str) -> Optional["ShowFilm"]:
        row = await cls.pool.fetch(cls.GET_SHOW_BY_NAME, title)
        return ShowFilm(*(row[0])) if row else None

    @classmethod
    async def get_show_by_id(cls, pk: int) -> Optional["ShowFilm"]:
        row = await cls.pool.fetch(cls.GET_SHOW_BY_ID, pk)
        return ShowFilm(*(row[0])) if row else None

    @classmethod
    async def get_show_by_id_full(cls, pk: int) -> Optional["ShowFilm"]:
        row = await cls.pool.fetch(cls.GET_SHOW_BY_ID_FULL, pk)
        return ShowFilm(*(row[0])) if row else None

    @classmethod
    async def get_random_show_full(cls) -> Optional["ShowFilm"]:
        row = await cls.pool.fetch(cls.GET_RANDOM_FILM)
        return ShowFilm(*(row[0])) if row else None

    @classmethod
    async def get_shows(cls, date: datetime, limit: int) -> List["ShowFilm"]:
        rows = await cls.pool.fetch(cls.GET_SHOWS, date, limit)
        return [ShowFilm(*(row)) for row in rows] if rows else []

    @classmethod
    async def get_shows_filter_date(cls, date_start: datetime, date_end: datetime, limit: int) -> List["ShowFilm"]:
        rows = await cls.pool.fetch(cls.GET_SHOWS_FILTER_DATE, date_start, date_end, limit)
        return [ShowFilm(*(row)) for row in rows] if rows else []

    @classmethod
    async def get_shows_recommended_by_clients(cls, date: datetime, limit: int, offset: int) -> List[int]:
        rows = await cls.pool.fetch(cls.GET_RECOMMENDED_FILM_BY_CLIENTS, date, limit, offset)
        return [row[0] for row in rows] if rows else []

    @classmethod
    async def get_show_watching_now(cls, date: datetime) -> List[int]:
        rows = await cls.pool.fetch(cls.GET_SHOWS_WATCHING_NOW, date)
        return [row[0] for row in rows] if rows else []

    @classmethod
    async def get_popular_films_by_date(cls, date: datetime) -> List["ShowFilm"]:
        rows = await cls.pool.fetch(cls.GET_POPULAR_FILM_BY_DATE, date)
        return [ShowFilm(*(row)) for row in rows] if rows else []


class GenreDB(ConfigurableDB):
    CREATE_TABLE = (
        """create table if not exists genre
            (
                id integer not null constraint genres_pk primary key,
                name varchar(50)
            );
        """
    )
    CREATE_ROW = (
        "INSERT INTO genre (id, name) VALUES ($1, $2);"
    )
    SELECT_BY_ID = (
        "select id, name from genre where id = $1"
    )
    SELECT_GENRES_BY_MOVIE_ID = (
        "SELECT gr.name FROM film_genre fg left JOIN genre gr on fg.genre_id=gr.id WHERE fg.film_id=$1"
    )

    @classmethod
    async def create_table(cls):
        await cls.pool.execute(cls.CREATE_TABLE)

    @classmethod
    async def create(cls, pk: int, name: str):
        await cls.pool.execute(cls.CREATE_ROW, pk, name)

    @classmethod
    async def get_by_pk(cls, pk: int) -> Optional["Genre"]:
        row = await cls.pool.fetch(cls.SELECT_BY_ID, pk)
        return Genre(*(row[0])) if row else None

    @classmethod
    async def get_genres_by_movie_id(cls, pk: int) -> List["str"]:
        rows = await cls.pool.fetch(cls.SELECT_GENRES_BY_MOVIE_ID, pk)
        return rows if rows else []


class LanguageDB(ConfigurableDB):
    CREATE_TABLE = (
        """create table if not exists languages
            (
                id integer not null constraint languages_pk primary key,
                name varchar(50)
            );
        """
    )
    CREATE_ROW = (
        "INSERT INTO languages (id, name) VALUES ($1, $2);"
    )
    SELECT_BY_ID = (
        "select id, name from languages where id = $1"
    )
    SELECT_BY_NAME = (
        "select id, name from languages where name = $1"
    )

    @classmethod
    async def create_table(cls):
        await cls.pool.execute(cls.CREATE_TABLE)

    @classmethod
    async def create(cls, pk: int, name: str):
        await cls.pool.execute(cls.CREATE_ROW, pk, name)

    @classmethod
    async def get_by_pk(cls, pk: int) -> Optional["Languages"]:
        row = await cls.pool.fetch(cls.SELECT_BY_ID, pk)
        return Languages(*(row[0])) if row else None

    @classmethod
    async def get_by_name(cls, name: str) -> Optional["Languages"]:
        row = await cls.pool.fetch(cls.SELECT_BY_NAME, name)
        return Languages(*(row[0])) if row else None


class FilmGenreDB(ConfigurableDB):
    CREATE_TABLE = (
        """
        CREATE TABLE film_genre (
        film_id int8 NOT NULL,
        genre_id int4 NOT NULL,
        CONSTRAINT film_genres_genres_id_fk FOREIGN KEY (genre_id) REFERENCES genre(id),
        CONSTRAINT film_genres_showfilm_id_fk FOREIGN KEY (film_id) REFERENCES showfilm(id) ON DELETE CASCADE,
        PRIMARY KEY (film_id,genre_id)
        );
        """
    )
    CREATE_ROW = (
        """INSERT INTO film_genre (film_id, genre_id) VALUES ($1, $2);"""
    )

    @classmethod
    async def create_table(cls):
        await cls.pool.execute(cls.CREATE_TABLE)

    @classmethod
    async def create(cls, film_id: int, genre_id: int):
        await cls.pool.execute(cls.CREATE_ROW, film_id, genre_id)


class FavoriteDB(ConfigurableDB):
    CREATE_TABLE = (
        """CREATE TABLE favorites (
            client_id int8 NOT NULL,
            show_id int8 NOT NULL,
            CONSTRAINT favorites_client_id_fk FOREIGN KEY (client_id) REFERENCES client(id),
            CONSTRAINT favorites_showfilm_id_fk FOREIGN KEY (show_id) REFERENCES showfilm(id),
            PRIMARY KEY (client_id,show_id)
        );
        """
    )
    CREATE_ROW = (
        """INSERT INTO favorites (client_id, show_id) values ($1, $2)"""
    )
    DELETE_ROW = (
        """DELETE FROM favorites WHERE client_id = $1 and show_id = $2"""
    )

    SELECT_ROWS = (
        """SELECT show_id FROM favorites where client_id = $1"""
    )

    GET_FAVORITES_SIMILAR_USERS_BY_CLIENT = (
        """ SELECT r1.client_id AS clients_id , r1.show_id as film_id
                    ,COUNT(r1.show_id) AS likes_match
                    ,count(*) AS total_match
                    FROM favorites AS r1
                    JOIN favorites AS r2 ON r2.client_id = $1 AND r1.show_id = r2.show_id
                    WHERE r1.client_id <> $1 GROUP BY r1.show_id, r1.client_id
                    ORDER BY r1.client_id limit 50000;"""
    )
    GET_FAVORITES_SIMILAR_USERS_BY_CLIENT_V2 = (
        """SELECT client_id,show_id FROM favorites WHERE client_id in (SELECT r1.client_id AS clients_id 
                    FROM favorites AS r1
                    JOIN favorites AS r2 ON r2.client_id = $1 AND r1.show_id = r2.show_id
                    WHERE r1.client_id <> $1 GROUP BY  r1.client_id
                    ORDER BY r1.client_id limit 50000);"""
    )
    GET_FAVORITES_SIMILAR_USERS_BY_CLIENT_V3 = (
        """ SELECT ro.client_id,ro.show_id,sf.title FROM favorites ro JOIN showfilm sf on ro.show_id = sf.id 
            WHERE client_id in (SELECT r1.client_id AS clients_id 
                    FROM favorites AS r1
                    JOIN favorites AS r2 ON r2.client_id = $1 AND r1.show_id = r2.show_id
                    WHERE r1.client_id <> $1 GROUP BY  r1.client_id
                    ORDER BY r1.client_id limit 50000) """
    )

    GET_OWN_FAVORITES = (
        """SELECT show_id,title from favorites JOIN showfilm sf on show_id =sf.id where client_id = $1"""
    )

    @classmethod
    async def create_table(cls):
        await cls.pool.execute(cls.CREATE_TABLE)

    @classmethod
    async def create(cls, client_id: int, show_id: int):
        await cls.pool.execute(cls.CREATE_ROW, client_id, show_id)

    @classmethod
    async def delete(cls, client_id: int, show_id: int):
        await cls.pool.execute(cls.DELETE_ROW, client_id, show_id)

    @classmethod
    async def get_favorites_ids_by_id(cls, client_id: int):
        rows = await cls.pool.fetch(cls.SELECT_ROWS, client_id)
        return [row[0] for row in rows] if rows else []

    @classmethod
    async def get_fav_similar_users_by_client_id(cls, client_id: int):
        rows = await cls.pool.fetch(cls.GET_FAVORITES_SIMILAR_USERS_BY_CLIENT, client_id)
        return rows if rows else []

    @classmethod
    async def get_fav_similar_users_by_client_id_v2(cls, client_id: int):
        rows = await cls.pool.fetch(cls.GET_FAVORITES_SIMILAR_USERS_BY_CLIENT_V2, client_id)
        return rows if rows else []

    @classmethod
    async def get_fav_similar_users_by_client_id_v3_with_titles(cls, client_id: int):
        rows = await cls.pool.fetch(cls.GET_FAVORITES_SIMILAR_USERS_BY_CLIENT_V3, client_id)
        return rows if rows else []

    @classmethod
    async def get_own_fav_shows(cls, client_id: int):
        rows = await cls.pool.fetch(cls.GET_OWN_FAVORITES, client_id)
        return rows if rows else []


async def prepare_db(*args, **kwargs):
    pool = await asyncpg.create_pool(user=DBConfig.DB_USER, password=DBConfig.DB_PASSWORD,
                                     database=DBConfig.DB_DATABASE, host=DBConfig.DB_HOST)
    ShowFilmDB.configurate(pool)
    GenreDB.configurate(pool)
    LanguageDB.configurate(pool)
    FilmGenreDB.configurate(pool)
    FavoriteDB.configurate(pool)
    ClientDB.configurate(pool)
    # await GenreDB.create_table()

    # NetflixDB.configurate(pool)
    # Reviews_MovieDB.configurate(pool)
    # ClientDb.configurate(pool)
    #
    # await NetflixDB.create_table()
    # await Reviews_MovieDB.create_table()
    # await ClientDb.create_table()
