import requests
import os
import spotipy.util as util
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from sqlalchemy import create_engine
from configparser import ConfigParser
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import hashlib
import boto3
from io import StringIO






'''
client_id = 'ca7fbe12e14546cb94ec1ec90c536bce'
client_secret = 'bf412cc6cd4a458da9706b4c4e83e258'

username = 'x5raulz6ufun7mia2v0s6oqeq'
scope = "user-library-read"
redirect_uri = "http://localhost:7777/callback"


auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
access_token = auth_manager.get_access_token(as_dict=False)
print(access_token)
'''


client_id = 'ca7fbe12e14546cb94ec1ec90c536bce'
client_secret = 'bf412cc6cd4a458da9706b4c4e83e258'

username = 'x5raulz6ufun7mia2v0s6oqeq'
scope = "user-library-read"
redirect_uri = "http://localhost:7777/callback"

auth_manager = SpotifyOAuth(client_id=client_id,
                                client_secret=client_secret,
                                redirect_uri=redirect_uri,
                                scope=scope)
    # Get an access token
access_token = auth_manager.get_access_token()
access_token = access_token['access_token']
print(access_token)


######################

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
artist_id_exploded = []
genre_list_exploded = []
release_date_list = []
album_id_list =[]
album_image_list = []

headers = {
    'Authorization': f'Bearer {access_token}'
}



response = requests.get('https://api.spotify.com/v1/playlists/2s3AjBZ1npsuzzY2NcAXI7', headers=headers).json()
total = response['tracks']['total'] 
print("Total 'liked songs' found:", total)


items = response['tracks']['items']
#Extraction 

for j in items:
    dateAddd = j['added_at'] 
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
    album_release_date = j['track']['album']['release_date']
    release_date_list.append(album_release_date)
    album_id = j['track']['album']['id']
    album_id_list.append(album_id)
    if j['track']['album']['images']:
        album_image = j['track']['album']['images'][0]['url']
        album_image_list.append(album_image)
    else:
        album_image_list.append(None)
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
    #artist_list.append(', '.join(artist_names)) #artist name
    artist_list.append(artist_names) #artist name
    artist_id.append(artist_ids)
    genre_list.append(list(artist_genres))








for artistid, artist_names in artists_by_id.items():
    artist_id_exploded.append(artistid)
    artist_list_new.append(artist_names)
while len(genre_list) < len(song_list):
    genre_list_exploded.append([])
url = "https://api.spotify.com/v1/audio-features/"
for i in track_ids:
    urls = url + i
    res = requests.get(urls, i, headers=headers).json()
    dance_score = [res['id'],res['danceability'], res['energy'],res['key']
    ,res['loudness'],res['mode'],res['speechiness'],res['acousticness']
    ,res['instrumentalness'],res['liveness'],res['valence'], res['tempo'], res['time_signature']]
    track_features.append(dance_score)




# exploding genres and artists
for artistid, artist_names in artists_by_id.items():
    artist_id_exploded.append(artistid)
    artist_list_new.append(artist_names)

##########################################

#Ensure that the genre list has the same number of records as the other lists
while len(genre_list) < len(song_list):
    genre_list_exploded.append([])

##########################################






def extract_spotify_liked_songs():
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
    track_features = []
    artist_list_new=[]
    release_date_list = []
    album_id_list =[]
    album_image_list = []
    auth_manager = SpotifyOAuth(client_id=client_id,
                                client_secret=client_secret,
                                redirect_uri=redirect_uri,
                                scope=scope)
    access_token = auth_manager.get_access_token(as_dict=False)
    print(access_token)
    # Get the authorization token for the user
    # access_token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
    headers = {
    'Authorization': 'Bearer {}'.format(access_token)
    }
    response = requests.get('https://api.spotify.com/v1/playlists/2s3AjBZ1npsuzzY2NcAXI7', headers=headers).json()
    total = response['tracks']['total'] 
    print("Total 'liked songs' found:", total)
    all_items = response['tracks']['items']
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
            album_release_date = j['track']['album']['release_date']
            release_date_list.append(album_release_date)
            album_id = j['track']['album']['id']
            album_id_list.append(album_id)
            if j['track']['album']['images']:
                album_image = j['track']['album']['images'][0]['url']
                album_image_list.append(album_image)
            else:
                album_image_list.append(None)
            artists = j['track']['album']['artists']
            artist_names = []
            artist_list_exploded = []
            artist_ids = []
            for artist in artists:
                artist_names.append(artist['name'])
                artist_ids.append(artist['id'])
                if artist['id'] in artists_by_id:
                    if artist['name'] not in artists_by_id[artist['id']]:
                        artists_by_id[artist['id']].append(artist['name'])
                else:
                    artists_by_id[artist['id']] = [artist['name']]
            #artist_list.append(', '.join(artist_names)) 
            artist_list.append(artist_names)
            artist_id.append(artist_ids)    
        for artistid, artist_names in artists_by_id.items():
            artist_list_exploded.append(artistid)
            artist_list_new.append(artist_names)
        url = "https://api.spotify.com/v1/audio-features/"
        for i in track_ids:
            urls = url + i
            res = requests.get(urls, i, headers=headers).json()
            dance_score = [res['id'],res['danceability'], res['energy'],res['key']
            ,res['loudness'],res['mode'],res['speechiness'],res['acousticness']
            ,res['instrumentalness'],res['liveness'],res['valence'], res['tempo']]
            track_features.append(dance_score)
        genre_by_artists = []
        url2 = "https://api.spotify.com/v1/artists/" 
        for i in artist_list_exploded:
            genre_url = url2 + i
            response2 = requests.get(genre_url, headers=headers)
            if response2.status_code == 200:
                artist = response2.json()
                genre_by_artists.append(artist['genres'])
            else:
                print(f"Error: {response2.status_code}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    return artist_list_exploded, song_list, album_list, \
        artist_list, track_features, add, track_ids, \
            artist_ids, artist_id, artist_list_new, artists_by_id, \
    album_id_list, release_date_list, album_image_list, genre_by_artists 
            











































def extract_spotify_liked_songs():
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
    track_features = []
    artist_list_new=[]
    release_date_list = []
    album_id_list =[]
    album_image_list = []
    auth_manager = SpotifyOAuth(client_id=client_id,
                                client_secret=client_secret,
                                redirect_uri=redirect_uri,
                                scope=scope)
    access_token = auth_manager.get_access_token(as_dict=False)
    print(access_token)
    # Get the authorization token for the user
    # access_token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
    headers = {
    'Authorization': 'Bearer {}'.format(access_token)
    }
    response = requests.get('https://api.spotify.com/v1/me/tracks', headers=headers).json()
    total = response['total'] 
    print("Total 'liked songs' found:", total)
    #total
    #print("Total 'liked songs' found:", total)
    for offset in range(0, total, 20):
        url = "https://api.spotify.com/v1/me/tracks?offset="+str(offset) + "&limit=20" 
        response1 = requests.get(url, headers=headers).json()
        getter = response1['items']
        all_items.extend(getter)
    print("Processing all", total ,"songs !!")
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
            album_release_date = j['track']['album']['release_date']
            release_date_list.append(album_release_date)
            album_id = j['track']['album']['id']
            album_id_list.append(album_id)
            if j['track']['album']['images']:
                album_image = j['track']['album']['images'][0]['url']
                album_image_list.append(album_image)
            else:
                album_image_list.append(None)
            artists = j['track']['album']['artists']
            artist_names = []
            artist_list_exploded = []
            artist_ids = []
            for artist in artists:
                artist_names.append(artist['name'])
                artist_ids.append(artist['id'])
                if artist['id'] in artists_by_id:
                    if artist['name'] not in artists_by_id[artist['id']]:
                        artists_by_id[artist['id']].append(artist['name'])
                else:
                    artists_by_id[artist['id']] = [artist['name']]
            #artist_list.append(', '.join(artist_names)) 
            artist_list.append(artist_names)
            artist_id.append(artist_ids)    
        for artistid, artist_names in artists_by_id.items():
            artist_list_exploded.append(artistid)
            artist_list_new.append(artist_names)
        url = "https://api.spotify.com/v1/audio-features/"
        for i in track_ids:
            urls = url + i
            res = requests.get(urls, i, headers=headers).json()
            dance_score = [res['id'],res['danceability'], res['energy'],res['key']
            ,res['loudness'],res['mode'],res['speechiness'],res['acousticness']
            ,res['instrumentalness'],res['liveness'],res['valence'], res['tempo']]
            track_features.append(dance_score)
        genre_by_artists = []
        url2 = "https://api.spotify.com/v1/artists/" 
        for i in artist_list_exploded:
            genre_url = url2 + i
            response2 = requests.get(genre_url, headers=headers)
            if response2.status_code == 200:
                artist = response2.json()
                genre_by_artists.append(artist['genres'])
            else:
                print(f"Error: {response2.status_code}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    return artist_list_exploded, song_list, album_list, \
        artist_list, track_features, add, track_ids, \
            artist_ids, artist_id, artist_list_new, artists_by_id, \
    album_id_list, release_date_list, album_image_list, genre_by_artists 
            



artist_list_exploded, \
    song_list, album_list, artist_list, track_features, add, \
        track_ids, artist_ids, artist_id, artist_list_new, artists_by_id,\
             album_id_list, release_date_list, \
                album_image_list, genre_by_artists = extract_spotify_liked_songs()






#Transformation
master_data = {'date_added': add,
        'track_id': track_ids,
        'song_name': song_list,
        'album_name': album_list,
        'ablum_id':album_id_list,
        'release_date':release_date_list,
        'cover_art':album_image_list,
        'artist': artist_list,
        'artist_id': artist_id
        }

df_master = pd.DataFrame(master_data)

############################## track features ############################## 
#fact_features
df_features = pd.DataFrame(track_features)
df_features.columns=['track_id','danceability','energy','key','loudness','mode'\
                     ,'speechiness','acousticness', 'instrumentalness','liveness','valence', 'tempo']

#df_features.to_csv('d:/projects_de/spotify_dimensional_modeling/fact_track_features.csv')
# merge master_data and df_features
df_grand_master = pd.merge(df_master, df_features, on='track_id')
#df_grand_master.to_csv('d:/projects_de/spotify_dimensional_modeling/great_master_data.csv', index=False)


############################## album ##############################
#dim_album
df_album = pd.DataFrame({"album_id": album_id_list, "album_name":album_list, "release_date": release_date_list, "cover_art": album_image_list})
#df_album.to_csv('d:/projects_de/spotify_dimensional_modeling/dim_album.csv', index=False)

############################## artist ##############################
#DIM_ARTISTS
df_artists = pd.DataFrame({"artist_id": artist_list_exploded, "artist_name": artist_list_new})
df_artists['artist_name'] = df_artists['artist_name'].str.join(', ')
#df_artists.to_csv('d:/projects_de/spotify_dimensional_modeling/dim_artists.csv', index=False)
#df_artists = df_artists.reset_index().rename(columns={'index': 'artist_key'})df_artists['artist_key'] += 1  # add 1 to the index values

############################## tracks ##############################
#DIM_TRACKS
df_tracks = pd.DataFrame({"track_id": track_ids, "album_id": album_id_list,"track_name": song_list, "album_name": album_list})
#df_tracks = df_tracks.reset_index().rename(columns={'index': 'track_key'})df_tracks['track_key'] += 1  # add 1 to the index values
#df_tracks.to_csv('d:/projects_de/spotify_dimensional_modeling/dim_tracks.csv', index=False)

#exploding

data = [(i, artist, artist_id) for i, row in df_master.iterrows() for artist, artist_id in zip(row['artist'], row['artist_id'])]
df_exploded = pd.DataFrame(data, columns=['index', 'artist', 'artist_id']).set_index('index')
df_master_exploded = pd.merge(df_master.drop(['artist', 'artist_id'], axis=1), df_exploded, left_index=True, right_index=True)
## creating a bridge table
df_artist_track_bridge = df_master_exploded[['track_id', 'artist_id','song_name', 'artist']].drop_duplicates()

#bridge_track_artist [bridge]
df_track_artist_bridge = df_artist_track_bridge.drop(['song_name', 'artist'], axis=1)
#df_artist_track_bridge.to_csv('d:/projects_de/spotify_dimensional_modeling/track_artist_prebridge.csv', index=False)
#df_track_artist_bridge.to_csv('d:/projects_de/spotify_dimensional_modeling/fact_track_artist_bridge.csv', index=False)

############################## Genre ##############################
df_artist_genres = pd.DataFrame({"artist_id":artist_list_exploded, "artist":artist_list_new, "genres":genre_by_artists})
df_artist_genres['artist'] = df_artist_genres['artist'].str.join(', ')
df_artist_genres = df_artist_genres.drop(['artist'], axis = 1)
df_artist_genres = df_artist_genres.explode('genres').drop_duplicates()
'''
distinct_genres = df_artist_genres['genres'].unique()
distinct_genres = pd.DataFrame(distinct_genres).dropna()
distinct_genres.columns = ['genres']
distinct_genres = distinct_genres.reset_index(drop=True)
distinct_genres['genre_key'] = distinct_genres.index + 1
'''
#Distinct genres
#dim_genre
distinct_genres = df_artist_genres['genres'].unique()
distinct_genres = pd.DataFrame(distinct_genres)
distinct_genres.columns = ['genres']
distinct_genres = distinct_genres.reset_index().rename(columns={'index': 'genre_key'})
distinct_genres['genre_key'] += 1  # add 1 to the index values
#df_artist_genres.to_csv('d:/projects_de/spotify_dimensional_modeling/dim_artist_genres.csv', index = False)

#bridge_artist_genre [bridge]
df_artist_genres_bridge = pd.merge(df_artist_genres, distinct_genres, on = 'genres').drop(['genres'], axis = 1)

#bridge_track_genre [bridge]
df_track_genre_bridge = pd.merge(df_track_artist_bridge, df_artist_genres_bridge, on = 'artist_id').drop(['artist_id'], axis = 1)
 

############################## DIM_DATE ##############################
#dim_date
df_date = pd.DataFrame({"datetime" : pd.to_datetime(add)})
df_date["date_added"] = df_date["datetime"].dt.date
df_date["time_added"] = df_date["datetime"].dt.time
df_date['timezone'] = df_date['datetime'].dt.tz.zone
df_date.insert(loc=0, column='track_id', value=track_ids) 
df_date = df_date.reset_index().rename(columns={'index': 'date_key'})
df_date['date_key'] += 1  # add 1 to the index values
#df_date.to_csv('d:/projects_de/spotify_dimensional_modeling/dim_date.csv', index=False)

'''
distinct values of each columns of the df_tracks_genres df
nunique = df_tracks_genres.nunique()

# Display the number of distinct values for each column
print(nunique)
'''

############################## S3Upload ##############################


aws_access_key_id = 'AKIAXZ2AHA47QQWJFNPF'
aws_secret_access_key = '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'

# Create an S3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# Set the name of your S3 bucket and the file path where you want to save the CSV file
bucket_name = 's3numerone'
file_path = 'master_data.csv'

# Convert your dataframe to a CSV string
csv_buffer = StringIO()
df_grand_master.to_csv(csv_buffer, index=False)

# Upload the CSV file to the S3 bucket
s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_path)

db_user = 'milan'
db_password = '3231'
db_host = 'localhost'
db_port = '5433'
db_name = 'pipelines'

# Create the connection string
connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

# Create the engine
engine = create_engine(connection_string)

connection = engine.connect()

connection.execute("drop table if exists master_sp.dim_details_large cascade;")
#fact_track_features
df_features.to_sql('fact_track_features', engine, schema='master_sp', if_exists='replace', index=False)
#dim_genres
distinct_genres.to_sql('dim_genres', engine, schema='master_sp', if_exists='replace', index=False)
#dim_album
df_album.to_sql('dim_album', engine, schema='master_sp', if_exists='replace', index=False)
#dim_artists
df_artists.to_sql('dim_artists', engine, schema='master_sp', if_exists='replace', index=False)
#dim_tracks
df_tracks.to_sql('dim_tracks', engine, schema='master_sp', if_exists='replace', index=False)
#fact_track_artist_bridge
df_track_artist_bridge.to_sql('track_artist_bridge', engine, schema='master_sp', if_exists='replace', index=False)
#track_artist_prebridge
df_artist_genres_bridge.to_sql('artist_genres_bridge', engine, schema='master_sp', if_exists='replace', index=False)
#track_artist_prebridge
df_track_genre_bridge.to_sql('track_genre_bridge', engine, schema='master_sp', if_exists='replace', index=False)
#dim_date
df_date.to_sql('dim_date', engine, schema='master_sp', if_exists='replace', index=False)





df_artist_genres_bridge


df_track_genre_bridge

##############################################################
            
###############################################################






'''
def extract_spotify_liked_songs():
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
    print(access_token)
    # Get the authorization token for the user
    # access_token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
    headers = {
    'Authorization': 'Bearer {}'.format(access_token)
    }
    response = requests.get('https://api.spotify.com/v1/me/tracks', headers=headers).json()
    total = response['total'] 
    print("Total 'liked songs' found:", total)
    #total
    #print("Total 'liked songs' found:", total)
    for offset in range(0, total, 20):
        url = "https://api.spotify.com/v1/me/tracks?offset="+str(offset) + "&limit=20" 
        response1 = requests.get(url, headers=headers).json()
        getter = response1['items']
        all_items.extend(getter)
    print("Processing all", total ,"songs !!")
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
            artist_id_exploded = []
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
                    artist_genres.extend([])
            artist_id.append(artist_ids)
            artist_list.append(', '.join(artist_names)) #artist name
            genre_list.append(list(artist_genres))    
        for artistid, artist_names in artists_by_id.items():
            artist_id_exploded.append(artistid)
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
    return song_list, album_list, artist_list, genre_list, track_features, add, track_ids, artist_ids, artist_id, artist_genres, artist_list_new, artists_by_id, artist_id_exploded
    

song_list, album_list, artist_list, genre_list, track_features, \
    add, track_ids, artist_id_exploded, \
    artist_ids, artist_id, artist_genres, artist_list_new, \
        artists_by_id = extract_spotify_liked_songs()

print("extraction done")
'''





