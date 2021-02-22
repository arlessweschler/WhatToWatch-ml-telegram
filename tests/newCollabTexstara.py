import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

from database.DBService import FavoriteDB


class CollaborativeFilter:
    client_id: int

    def __init__(self, client_id):

        self.clients_id = []
        self.films_id = []
        self.ratings = []
        self.titles = []
        self.client_id = client_id

        pass

    async def get_movies_id_after(self):
        data = await FavoriteDB.get_fav_similar_users_by_client_id_v3_with_titles(self.client_id)
        own_client_data = await FavoriteDB.get_own_fav_shows(self.client_id)

        for row in own_client_data:
            self.clients_id.append(self.client_id)
            self.films_id.append(row[0])
            self.titles.append(row[1])
            self.ratings.append(5)

        for row in data:
            self.clients_id.append(row[0])
            self.films_id.append(row[1])
            self.titles.append(row[2])
            self.ratings.append(5)

        

        self.df_ratings = pd.DataFrame(
            data={
                'userId': self.clients_id,
                'movieId': self.films_id,
                'rating': self.ratings
            }
        )
        movies = pd.DataFrame(
            data={
                'movieId': self.films_id,
                'title': self.titles
            }
        )
        self.df_movies = movies.drop_duplicates(subset=['movieId'])
        return await self.get_recommend(self.client_id)

    async def get_recommend(self, client_id):
        movies = self.df_movies
        Ratings = self.df_ratings

        Mean = Ratings.groupby(by="userId", as_index=False)['rating'].mean()
        Rating_avg = pd.merge(Ratings, Mean, on='userId')
        Rating_avg['adg_rating'] = Rating_avg['rating_x']

        check = pd.pivot_table(Rating_avg, values='rating_x', index='userId', columns='movieId')

        final = pd.pivot_table(Rating_avg, values='adg_rating', index='userId', columns='movieId')

        final_movie = final.fillna(final.mean(axis=0))

        final_user = final.apply(lambda row: row.fillna(0), axis=1)

        b = cosine_similarity(final_user)
        np.fill_diagonal(b, 0)
        similarity_with_user = pd.DataFrame(b, index=final_user.index)
        similarity_with_user.columns = final_user.index
        similarity_with_user.head()

        cosine = cosine_similarity(final_movie)
        np.fill_diagonal(cosine, 0)
        similarity_with_movie = pd.DataFrame(cosine, index=final_movie.index)
        similarity_with_movie.columns = final_user.index

        def find_n_neighbours(df, n):
            order = np.argsort(df.values, axis=1)[:, :n]
            df = df.apply(lambda x: pd.Series(x.sort_values(ascending=False)
                                              .iloc[:n].index,
                                              index=['top{}'.format(i) for i in range(1, n + 1)]), axis=1)
            return df

        sim_user_30_m = find_n_neighbours(similarity_with_movie, 30)

        Rating_avg = Rating_avg.astype({"movieId": str})
        Movie_user = Rating_avg.groupby(by='userId')['movieId'].apply(lambda x: ','.join(x))

        def User_item_score1(user):
            Movie_seen_by_user = check.columns[check[check.index == user].notna().any()].tolist()
            a = sim_user_30_m[sim_user_30_m.index == user].values
            b = a.squeeze().tolist()
            d = Movie_user[Movie_user.index.isin(b)]
            l = ','.join(d.values)
            Movie_seen_by_similar_users = l.split(',')
            Movies_under_consideration = list(
                set(Movie_seen_by_similar_users) - set(list(map(str, Movie_seen_by_user))))
            Movies_under_consideration = list(map(int, Movies_under_consideration))
            score = []
            for item in Movies_under_consideration:
                c = final_movie.loc[:, item]
                d = c[c.index.isin(b)]
                f = d[d.notnull()]
                avg_user = Mean.loc[Mean['userId'] == user, 'rating'].values[0]
                index = f.index.values.squeeze().tolist()
                corr = similarity_with_movie.loc[user, index]
                fin = pd.concat([f, corr], axis=1)
                fin.columns = ['adg_score', 'correlation']
                fin['score'] = fin.apply(lambda x: x['adg_score'] * x['correlation'], axis=1)
                nume = fin['score'].sum()
                deno = fin['correlation'].sum()
                final_score = avg_user + (nume / deno)
                score.append(final_score)
            data = pd.DataFrame({'movieId': Movies_under_consideration, 'score': score})
            top_10_recommendation = data.sort_values(by='score', ascending=False).head(10)
            Movie_Name = top_10_recommendation.merge(movies, how='inner', on='movieId')
            Movie_Names = Movie_Name.movieId.values.tolist()
            return Movie_Names

        return User_item_score1(client_id)
# user = int('93000')
# predicted_movies = User_item_score1(user)
# for i in predicted_movies:
#     print(i)
