import pandas as pd
from pathlib import Path
import numpy as np

#Create directory if not exists
Path("ToNeo4").mkdir(parents=True, exist_ok=True)
Path("ToRel").mkdir(parents=True, exist_ok=True)

#Load csv
df = pd.read_csv('netflix_titles.csv')
df['show_id'] = df['show_id'].str[1:].astype(int)

#Medias
media = df[['show_id','title','description']].replace('', np.nan).dropna()

#Director + edges
directors_edges = df[['show_id', 'director']].dropna()
directors = directors_edges['director'].drop_duplicates()

#Cast + edges
cast_edges = df[['show_id','cast']].dropna()
cast_edges['cast'] = cast_edges['cast'].apply(lambda x: x.split(','))
cast_edges = cast_edges.explode('cast').drop_duplicates()
cast_edges['cast'] = cast_edges['cast'].apply(lambda x: x.lstrip(' '))
cast_edges = cast_edges.replace('', np.nan).dropna()
cast_member = cast_edges['cast']

#Cast + director combined
persons = directors.append(cast_member,ignore_index=True)
persons = pd.DataFrame(persons, columns= ['name']).drop_duplicates()

#Countries + edges
country_edges = df[['show_id', 'country']].dropna()
country_edges['country'] = country_edges['country'].apply(lambda x: x.split(','))
country_edges = country_edges.explode('country').drop_duplicates()
country_edges['country'] = country_edges['country'].apply(lambda x: x.lstrip(' '))
country_edges = country_edges.replace('', np.nan).dropna()
countries = pd.DataFrame(country_edges['country'], columns=['country']).drop_duplicates()

#Type + edges
type_edges = df[['show_id', 'type']].dropna().drop_duplicates()
types = pd.DataFrame(type_edges['type'], columns=['type']).drop_duplicates()

#Date_added + edges
date_added_edges = df[['show_id', 'date_added']].dropna().drop_duplicates()
date_added = pd.DataFrame(date_added_edges['date_added'], columns=['date_added']).drop_duplicates()

#Release_year + edges
release_year_edges = df[['show_id', 'release_year']].dropna().drop_duplicates()
release_year = pd.DataFrame(release_year_edges['release_year'], columns=['release_year']).drop_duplicates()

#Rating + edges
rating_edges = df[['show_id', 'rating']].dropna().drop_duplicates()
rating_edges = rating_edges[~rating_edges.rating.str.endswith("min")]
rating = pd.DataFrame(rating_edges['rating'], columns=['rating']).drop_duplicates()

#Duration + edges
duration_edges = df[['show_id', 'duration']].dropna().drop_duplicates()
duration_edges['duration'] = duration_edges['duration'].str.split(n=1).str[0]
duration = pd.DataFrame(duration_edges['duration'], columns=['duration']).drop_duplicates()


#Put everything into csv
media.to_csv("ToNeo4/media.csv", sep=',',index=False)

directors_edges.to_csv("ToNeo4/directors_edges.csv", sep=',',index=False)
cast_edges.to_csv("ToNeo4/cast_edges.csv", sep=',',index=False)
persons.to_csv("ToNeo4/persons.csv", sep=',',index=False)

country_edges.to_csv("ToNeo4/country_edges.csv", sep=',',index=False)
countries.to_csv("ToNeo4/countries.csv", sep=',',index=False)

type_edges.to_csv("ToNeo4/type_edges.csv", sep=',',index=False)
types.to_csv("ToNeo4/types.csv", sep=',',index=False)

date_added_edges.to_csv("ToNeo4/date_added_edges.csv", sep=',',index=False)
date_added.to_csv("ToNeo4/date_added.csv", sep=',',index=False)

release_year_edges.to_csv("ToNeo4/release_year_edges.csv", sep=',',index=False)
release_year.to_csv("ToNeo4/release_year.csv", sep=',',index=False)

rating_edges.to_csv("ToNeo4/rating_edges.csv", sep=',',index=False)
rating.to_csv("ToNeo4/rating.csv", sep=',',index=False)

duration_edges.to_csv("ToNeo4/duration_edges.csv", sep=',',index=False)
duration.to_csv("ToNeo4/duration.csv", sep=',',index=False)

'''
#---------------------------------------------------------------- Relationships for relationnal database

#Find and transform a value into its index
def transform(df1,df2,name1,name2):
    return df1.loc[df1[name1] == df2[name2]].index[0]

#Cast relationships
cast_rel = cast_edges
cast_rel['cast'] = cast_rel.apply(lambda x : transform(persons,x,'name','cast'), axis=1)
cast_rel.to_csv("ToRel/cast_rel.csv", sep=',',index=False)

#Director relationships
directors_rel = directors_edges
directors_rel['director'] = directors_rel.apply(lambda x : transform(persons,x,'name','director'), axis=1)
directors_rel.to_csv("ToRel/directors_rel.csv", sep=',',index=False)

#Country relationships
country_rel = country_edges
country_rel['country'] = country_rel.apply(lambda x : transform(countries,x,'country','country'), axis=1)
country_rel.to_csv("ToRel/country_rel.csv", sep=',',index=False)

#date_added relationships
date_added_rel = date_added_edges
date_added_rel['date_added'] = date_added_rel.apply(lambda x : transform(date_added,x,'date_added','date_added'), axis=1)
date_added_rel.to_csv("ToRel/date_added_rel.csv", sep=',',index=False)

#duration relationships
duration_rel = duration_edges
duration_rel['duration'] = duration_rel.apply(lambda x : transform(duration,x,'duration','duration'), axis=1)
duration_rel.to_csv("ToRel/duration_rel.csv", sep=',',index=False)

#rating relationships
rating_rel = rating_edges
rating_rel['rating'] = rating_rel.apply(lambda x : transform(rating,x,'rating','rating'), axis=1)
rating_rel.to_csv("ToRel/rating_rel.csv", sep=',',index=False)

#release_year relationships
release_year_rel = release_year_edges
release_year_rel['release_year'] = release_year_rel.apply(lambda x : transform(release_year,x,'release_year','release_year'), axis=1)
release_year_rel.to_csv("ToRel/release_year_rel.csv", sep=',',index=False)

#type relationships
type_rel = type_edges
type_rel['type'] = type_rel.apply(lambda x : transform(types,x,'type','type'), axis=1)
type_rel.to_csv("ToRel/type_rel.csv", sep=',',index=False)


'''