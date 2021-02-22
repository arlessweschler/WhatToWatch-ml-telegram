import datetime
from typing import List

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

from database.DBService import ShowFilmDB
from models.ShowFilm import ShowFilm


class ViaTitleRecommendationSystem:
    date_start: datetime=None
    date_end: datetime=None
    show_films: List['ShowFilm']
    limit: int = 12000
    movie: ShowFilm

    def __init__(self, movie: ShowFilm, date_start=None, date_end=None):
        self.movie = movie
        if date_start is not None and date_end is not None:
            self.date_start = date_start
            self.date_end = date_end
        else:
            date = movie.release_date + datetime.timedelta(weeks=48)
            self.date_start = date

    async def prepare_predict(self):
        if self.date_end is not None:
            self.show_films = await ShowFilmDB.get_shows_filter_date(date_start=self.date_start,
                                                                     date_end=self.date_end,
                                                                     limit=self.limit)
        else:
            self.show_films = await ShowFilmDB.get_shows(date=self.date_start, limit=self.limit)

    async def predict(self):
        pks = []
        titles = []
        overviews = []
        for i in self.show_films:
            pks.append(i.id)
            titles.append(i.title)
            overviews.append(i.description)

        data = {
            'id': pks,
            'title': titles,
            'overview': overviews
        }
        df = pd.DataFrame(data=data)

        tfidf = TfidfVectorizer(stop_words='english')
        tfidf_matrix = tfidf.fit_transform(df['overview'])

        cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)

        indices = pd.Series(df.index, index=df['title']).drop_duplicates()
        title = self.movie.title

        try:
            idx = indices[title]

            sim_scores = list(enumerate(cosine_sim[idx]))
            sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
            sim_scores = sim_scores[1:11]

            shows_ordered = [self.show_films[i[0]] for i in sim_scores]

            return shows_ordered
        except Exception as e:
            print(e)
            return []
