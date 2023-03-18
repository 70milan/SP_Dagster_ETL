from dagster import Out, op
import requests
from configparser import ConfigParser
import os
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from sqlalchemy import create_engine
from sp_etl.db_conn import postgres_connection
from configparser import ConfigParser
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import hashlib

config = ConfigParser()
config.read(os.path.join(os.path.dirname(os.path.abspath(__name__)), 'sp_etl/database.ini'))

client_id = config.get('sp_creds', 'client_id')
client_secret = config.get('sp_creds', 'client_secret')
username = config.get('sp_creds', 'username')
scope = "user-library-read"
redirect_uri = "http://localhost:7777/callback"

@op(out={
    "song_list": Out(),
    "album_list": Out(),
    "genre_list": Out(),
    "track_features": Out(),
    "add": Out(),
    "track_ids": Out(),
    "artist_id": Out(),
    "artist_list_new": Out()
})
def extract_spotify_liked_songs(context):
    limit = 20
    offset = 0
    all_items = []
    add = []
    artists_by_id = {}
    song_list =[]
    artist_list = []
    artist_id = []
    album_list=[]
    track_ids=[]
    genre_list = []
    track_features = []
    artist_list_new=[]
    auth_manager = SpotifyOAuth(client_id=client_id,
                                client_secret=client_secret,
                                redirect_uri=redirect_uri,
                                scope=scope)
    access_token = auth_manager.get_access_token(as_dict=False)
    headers = {
    'Authorization': 'Bearer {}'.format(access_token)
    }
    response = requests.get('https://api.spotify.com/v1/me/tracks', headers=headers).json()
    total = response['total'] 
    context.log.info(f'{total} songs found')
    context.log.info(f'Processing {total} all songs')
    #total
    #print("Total 'liked songs' found:", total)
    for offset in range(0, total, 20):
        url = "https://api.spotify.com/v1/me/tracks?offset="+str(offset) + "&limit=20" 
        response1 = requests.get(url, headers=headers).json()
        getter = response1['items']
        all_items.extend(getter)
    try:
        for j in all_items:
            dateAddd = j['added_at'] 
            #dateAdd = dateAddd[0:10]#added date
            add.append(dateAddd)
            s_n = [j['track']['name']]
            Id = [j['track']['id']] #id
            identif = ' '.join(str(v) for v in Id) 
            track_ids.append(identif)
            song_name = ','.join(str(v) for v in s_n) 
            song_list.append(song_name) #tracks
            album = [j['track']['album']['name']]
            album1 = ' '.join(str(v) for v in album) 
            album_list.append(album1) #albums
            artists = j['track']['album']['artists']
            artist_names = []
            artist_ids = []
            artist_genres = set()
            for artist in artists:
                artist_names.append(artist['name'])
                artist_ids.append(artist['id'])
                if artist['id'] in artists_by_id:
                    if artist['name'] not in artists_by_id[artist['id']]:
                        artists_by_id[artist['id']].append(artist['name'])
                else:
                    artists_by_id[artist['id']] = [artist['name']]
                url = "https://api.spotify.com/v1/artists/" + artist['id']
                response2 = requests.get(url, headers=headers)
                if hasattr(response2, 'status_code'):
                    if response2.status_code == 200:
                        artist = response2.json()
                        g_n = artist['genres']
                        artist_genres.update(g_n)
                    elif response2.status_code == 429:
                        print("Error: Too many requests")
                    else:
                        print(f"Error: {response2.status_code}")
                else:
                    print("Error: Unable to retrieve artist information")
            artist_list.append(', '.join(artist_names)) #artist name
            genre_list.append(list(artist_genres))    
        for artistid, artist_names in artists_by_id.items():
            artist_id.append(artistid)
            artist_list_new.append(artist_names)
        while len(genre_list) < len(song_list):
            genre_list.append([])# Ensure that the genre list has the same number of records as the other lists
        url = "https://api.spotify.com/v1/audio-features/"
        for i in track_ids:
            urls = url + i
            res = requests.get(urls, i, headers=headers).json()
            dance_score = [res['id'],res['danceability'], res['energy'],res['key']
            ,res['loudness'],res['mode'],res['speechiness'],res['acousticness']
            ,res['instrumentalness'],res['liveness'],res['valence'], res['tempo']]
            track_features.append(dance_score)
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        context.log.info("Extracted data from Spotify API.")
    return song_list, album_list, genre_list, track_features, add, track_ids, artist_id, artist_list_new

@op(out={
    "df_original": Out(),
    "df_date": Out(),
    "df_artists_final": Out(),
    "df_unique_genres": Out(),
    "df_features": Out(),
})
def dataframes_transform(context,song_list, album_list, genre_list, track_features, add, track_ids, artist_id, artist_list_new):
    context.log.info("Data transformation initiated.")
    df_original = pd.DataFrame({"track_id": track_ids,"track_list":song_list,"album_name" : album_list})
    ##Date##
    #manipulating date and time
    df_date = pd.DataFrame({"datetime" : pd.to_datetime(add)})
    df_date["date_added"] = df_date["datetime"].dt.date
    df_date["time_added"] = df_date["datetime"].dt.time
    df_date['timezone'] = df_date['datetime'].dt.tz.zone
    df_date.insert(loc=0, column='track_id', value=track_ids) # Add the 'id' column to df_artists
    df_date["datetime_track_id"] = df_date["datetime"].astype(str) + "_" + df_date["track_id"].astype(str)
    df_date["datetime_track_id_hash"] = df_date["datetime_track_id"].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    df_date = df_date.drop(columns=['datetime_track_id'])
    df_date.set_index("datetime_track_id_hash", inplace=True)
    ###artist##
    #separating the artists and merging the df_artist to final-1 df
    df_artistsid = pd.DataFrame({"artist_id": artist_id })
    df_artistsid = df_artistsid.explode('artist_id').reset_index(drop=True)
    unique_artistsid = df_artistsid['artist_id'].unique()
    df_unique_artistsid = pd.DataFrame({'artist_unique_id': unique_artistsid})
    df_artists = pd.DataFrame({"artist_name":artist_list_new })
    #df_artists['artist_name'] = df_artists['artist_name'].str.split(',')
    df_artists = df_artists.explode('artist_name').reset_index(drop=True)
    unique_artistname = df_artists['artist_name'].unique()
    df_unique_artistname = pd.DataFrame({'artist_unique_name': unique_artistname})
    df_artists_final = pd.concat([df_unique_artistsid, df_unique_artistname], axis=1)
    #####################genre#######################
    #add columns to df_genre(separate table for genre)
    #max colmns
    df_genre_all = pd.DataFrame({"genre":genre_list})
    df_genre_all = df_genre_all.explode('genre').reset_index(drop=True)
    unique_genres = df_genre_all['genre'].unique()
    df_unique_genres = pd.DataFrame({'genre': unique_genres})
    #df_unique_genres.insert(loc=0, column='track_name', value=e.song_list) # Add the 'id' column to df_artists
    #(separate table for features)
    df_features = pd.DataFrame(track_features)
    df_features.columns=['track_id','danceability','energy','key','loudness','mode','speechiness','acousticness', 'instrumentalness','liveness','valence', 'tempo']
    context.log.info("Data transformation completed.")
    #df_features.insert(loc=0, column='track_list', value=song_list) # Add the 'id' column to df_artists
    return df_original, df_date, df_artists_final, df_unique_genres, df_features

@op
def load_to_postgres(context, df_original, df_date, df_artists_final, df_unique_genres, df_features):
    try:
        engine = postgres_connection()
    except Exception as e:
        print(f"Connection Uncessfull: {str(e)}")
    context.log.info("Loading has begun.")
    connection = engine.connect()
    connection.execute("drop table if exists master_sp.dim_details_large cascade;")
    #dim_everythin_part_one
    df_original.to_sql('dim_details_small', engine, schema='master_sp', if_exists='replace', index=False)
    #dim_date
    df_date.to_sql('dim_date', engine, schema='master_sp', if_exists='replace', index=False)
    #dim_track_artists
    df_artists_final.to_sql('dim_track_artists', engine, schema='master_sp', if_exists='replace', index=False)
    #dim_genres
    df_unique_genres.to_sql('dim_track_genres', engine, schema='master_sp', if_exists='replace', index=False)
    #fact_artist
    df_features.to_sql('fact_track_features', engine, schema='master_sp', if_exists='replace', index=False)
    context.log.info("load completed.")

  
'''
client_id = os.environ.get("SP_CLIENT_ID")
client_secret = os.environ.get("SP_CLIENT_SECRET")

config = ConfigParser()
config.read(os.path.join(os.path.abspath(os.path.dirname(__name__)), 'database.ini'))

try:
    # Get Postgres connection details from config
    postgres_user = config.get('postgres', 'user')
    postgres_password = config.get('postgres', 'password')
    postgres_host = config.get('postgres', 'host')
    postgres_port = config.get('postgres', 'port')
    postgres_database = config.get('postgres', 'database')
    engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
    print("Connected to Postgres database successfully!")
except Exception as e:
    print("Failed to connect to Postgres database: ", e)

'''

'''

@op
def load_to_postgres(,df_original, df_date, df_artists_final, df_unique_genres, df_features):
    connection = engine.connect()
    connection.execute("drop table if exists master_sp.dim_details_large cascade;")
    #dim_everythin_part_one
    df_original.to_sql('dim_details_small', engine, schema='master_sp', if_exists='replace', index=False)
    #dim_date
    df_date.to_sql('dim_date', engine, schema='master_sp', if_exists='replace', index=False)
    #dim_track_artists
    df_artists_final.to_sql('dim_track_artists', engine, schema='master_sp', if_exists='replace', index=False)
    #dim_genres
    df_unique_genres.to_sql('dim_track_genres', engine, schema='master_sp', if_exists='replace', index=False)
    #fact_artist
    df_features.to_sql('fact_track_features', engine, schema='master_sp', if_exists='replace', index=False)
    .log.info("load completed.")


    

limit = 20
offset = 0
all_items = []
add = []
artists_by_id = {}
song_list =[]
artist_list = []
artist_id = []
album_list=[]
track_ids=[]
genre_list = []
track_features = []
artist_list_new=[]
auth_manager = SpotifyOAuth(client_id=client_id,
        client_secret=client_secret,
           redirect_uri=redirect_uri,
           scope=scope)
access_token = auth_manager.get_access_token(as_dict=False)
headers = {
    'Authorization': 'Bearer {}'.format(access_token)
}
response = requests.get('https://api.spotify.com/v1/me/tracks', headers=headers).json()
total = response['total']
#total
print("Total 'liked songs' found:", total)

for offset in range(0, total, 20):
    url = "https://api.spotify.com/v1/me/tracks?offset="+str(offset) + "&limit=20" 
    response1 = requests.get(url, headers=headers).json()
    getter = response1['items']
    all_items.extend(getter)

for j in all_items:
    dateAddd = j['added_at'] 
        #dateAdd = dateAddd[0:10]#added date
    add.append(dateAddd)
    s_n = [j['track']['name']]
    Id = [j['track']['id']] #id
    identif = ' '.join(str(v) for v in Id) 
    track_ids.append(identif)
    song_name = ','.join(str(v) for v in s_n) 
    song_list.append(song_name) #tracks
    album = [j['track']['album']['name']]
    album1 = ' '.join(str(v) for v in album) 
    album_list.append(album1) #albums
    artists = j['track']['album']['artists']
    artist_names = []
    artist_ids = []
    artist_genres = set()
    
for artist in artists:
    artist_names.append(artist['name'])
    artist_ids.append(artist['id'])
    if artist['id'] in artists_by_id:
        if artist['name'] not in artists_by_id[artist['id']]:
            artists_by_id[artist['id']].append(artist['name'])
    else:
        artists_by_id[artist['id']] = [artist['name']]
    url = "https://api.spotify.com/v1/artists/" + artist['id']
    response2 = requests.get(url, headers=headers)
    
if response2.status_code == 200:
    artist = response2.json()
    g_n = artist['genres']
    artist_genres.update(g_n)
else:
    print(f"An error occurred: {str(e)}")

if hasattr(response2, 'status_code'):
    if response2.status_code == 200:
        artist = response2.json()
        g_n = artist['genres']
        artist_genres.update(g_n)
    elif response2.status_code == 429:
        print("Error: Too many requests")
    else:
        print(f"Error: {response2.status_code}")
else:
    print("Error: Unable to retrieve artist information")



artist_list.append(', '.join(artist_names)) #artist name
genre_list.append(list(artist_genres))    





for artistid, artist_names in artists_by_id.items():
    artist_id.append(artistid)
    artist_list_new.append(artist_names)

while len(genre_list) < len(song_list):
    genre_list.append([])# Ensure that the genre list has the same number of records as the other lists


url = "https://api.spotify.com/v1/audio-features/"

for i in track_ids:
    urls = url + i
    res = requests.get(urls, i, headers=headers).json()
    dance_score = [res['id'],res['danceability'], res['energy'],res['key']
    ,res['loudness'],res['mode'],res['speechiness'],res['acousticness']
    ,res['instrumentalness'],res['liveness'],res['valence'], res['tempo']]
    track_features.append(dance_score)








except Exception as e:
    print(f"An error occurred: {str(e)}")
return song_list, album_list, genre_list, track_features, add, track_ids, artist_id, artist_list_new
'''  
'''

@op(out={"df_original":  Out(),
         "df_date":  Out(),
         "df_artists_final":  Out(),
         "df_unique_genres":  Out(),
         "df_features":  Out()})
def dataframes_transform(,song_list, album_list, genre_list, track_features, add, track_ids, artist_id, artist_list_new):
    df_original = pd.DataFrame({"track_id": track_ids,"track_list":song_list,"album_name" : album_list})
    ##Date###manipulating date and time    
    df_date = pd.DataFrame({"datetime" : pd.to_datetime(add)})
    df_date["date_added"] = df_date["datetime"].dt.date
    df_date["time_added"] = df_date["datetime"].dt.time
    df_date['timezone'] = df_date['datetime'].dt.tz.zone
    df_date.insert(loc=0, column='track_id', value=track_ids) # Add the 'id' column to df_artists
    df_date["datetime_track_id"] = df_date["datetime"].astype(str) + "_" + df_date["track_id"].astype(str)
    df_date["datetime_track_id_hash"] = df_date["datetime_track_id"].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    df_date = df_date.drop(columns=['datetime_track_id'])
    df_date.set_index("datetime_track_id_hash", inplace=True)
    .log.info("Transformed DATE")
    ###artist##
    #separating the artists and merging the df_artist to final-1 df
    df_artistsid = pd.DataFrame({"artist_id": artist_id })
    df_artistsid = df_artistsid.explode('artist_id').reset_index(drop=True)
    unique_artistsid = df_artistsid['artist_id'].unique()
    df_unique_artistsid = pd.DataFrame({'artist_unique_id': unique_artistsid})
    df_artists = pd.DataFrame({"artist_name":artist_list_new })
    #df_artists['artist_name'] = df_artists['artist_name'].str.split(',')
    df_artists = df_artists.explode('artist_name').reset_index(drop=True)
    unique_artistname = df_artists['artist_name'].unique()
    df_unique_artistname = pd.DataFrame({'artist_unique_name': unique_artistname})
    df_artists_final = pd.concat([df_unique_artistsid, df_unique_artistname], axis=1)
    .log.info("Transformed ARTISTS")
    #####################genre#######################
    #add columns to df_genre(separate table for genre)
    #max colmns
    df_genre_all = pd.DataFrame({"genre":genre_list})
    df_genre_all = df_genre_all.explode('genre').reset_index(drop=True)
    unique_genres = df_genre_all['genre'].unique()
    df_unique_genres = pd.DataFrame({'genre': unique_genres})
    .log.info("Transformed GENRES")
    #df_unique_genres.insert(loc=0, column='track_name', value=e.song_list) # Add the 'id' column to df_artists
    #(separate table for features)
    df_features = pd.DataFrame(track_features)
    df_features.columns=['track_id','danceability','energy','key','loudness','mode','speechiness',\
                         'acousticness', 'instrumentalness','liveness','valence', 'tempo']
    .log.info("Transformed FEATURES")
    .log.info("Transformed completed.")
    #df_features.insert(loc=0, column='track_list', value=song_list) # Add the 'id' column to df_artists
    yield Output(df_original, "df_original")
    yield Output(df_date, "df_date")
    yield Output(df_artists_final, "df_artists_final")
    yield Output(df_unique_genres, "df_unique_genres")
    yield Output(df_features, "df_features")


'''