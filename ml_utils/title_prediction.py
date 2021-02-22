import asyncio
import datetime
from typing import List

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from rake_nltk import Rake
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from database.DBService import prepare_db, ShowFilmDB
from models.ShowFilm import ShowFilm


async def main():
    await prepare_db()
    date = datetime.datetime(2019, 1, 1) + datetime.timedelta(weeks=48)
    print(date)
    show_films = await ShowFilmDB.get_shows(date=date, limit=12000)

    predict(show_films)


def predict(shows: List["ShowFilm"]):
    pks = []
    titles = []
    overviews = []
    for i in shows:
        pks.append(i.id)
        titles.append(i.title)
        overviews.append(i.description)


    data = {
        'id': pks,
        'title': titles,
        'overview': overviews
    }

    df = pd.DataFrame(data=data)
    print(df['title'])
    print()
    print("-------------------------------------------")
    print()

    tfidf = TfidfVectorizer(stop_words='english')
    tfidf_matrix = tfidf.fit_transform(df['overview'])

    from sklearn.metrics.pairwise import linear_kernel

    # Compute the cosine similarity matrix
    cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)

    indices = pd.Series(df.index, index=df['title']).drop_duplicates()

    def get_recommendations(title, cosine_sim=cosine_sim):
        # Get the index of the movie that matches the title
        idx = indices[title]

        # Get the pairwsie similarity scores of all movies with that movie
        sim_scores = list(enumerate(cosine_sim[idx]))

        # Sort the movies based on the similarity scores
        sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)

        # Get the scores of the 10 most similar movies
        sim_scores = sim_scores[1:11]

        # Get the movie indices
        shows_ordered = [shows[i[0]] for i in sim_scores]

        return shows_ordered

    a = get_recommendations('Marvel s Jessica Jones')
    print("\n----------------------------\n")
    for show in a:
        print(show.id, show.title, show.release_date, show.popularity)
    # print(a)


asyncio.run(main())
