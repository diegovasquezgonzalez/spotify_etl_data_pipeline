# Setting up environment 
import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

#Defining function for getting album data in dictionary
def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {'album_id': album_id, 'name': album_name, 'release_date': album_release_date, 
                         'total_tracks': album_total_tracks, 'url': album_url}
        album_list.append(album_element)
    return album_list
    
#Defining function for getting artist data in dictionary
def artist(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == "track":
                for artist in value['artists']:
                    artist_dict={'artist_id': artist['id'], 'artist_name':artist['name'], 'external_url': artist['href']}
                    artist_list.append(artist_dict)
    return artist_list
                    
#Defining function for getting song data in dictionary
def song(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id': song_id, 'song_name': song_name, 'duration_ms': song_duration, 'url': song_url,
                        'popularity': song_popularity, 'song_added': song_added, 'album_id': album_id,
                        'artist_id': artist_id
                        }
        song_list.append(song_element)
    return song_list

#Defining function for data transformation
def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "etl-spotify-project-dv"
    Key = "raw_data/to_process/"
    
    spotify_data = []
    spotify_keys = []
    for file in s3.list_objects(Bucket = Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == ".json":
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']
            jsonObject= json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
            
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        song_list = song(data)
        
        #Album Dataframe
        album_df = pd.DataFrame.from_dict(album_list)
        #Artist Dataframe
        artist_df = pd.DataFrame.from_dict(artist_list)
        #Song Dataframe
        song_df = pd.DataFrame.from_dict(song_list)
        
        #Looking for and dropping duplicates
        album_df = album_df.drop_duplicates(subset=['album_id'])
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        #Convert release_date and song_added from object to datetime
        album_df['release_date'] = pd.to_datetime(album_df['release_date'])
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])
        
        #Create new song file with transformed data
        song_key = "transformed_data/song_data/song_transformed_" + str(datetime.now()) + ".csv"
        song_buffer = StringIO()
        song_df.to_csv(song_buffer)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = song_key, Body = song_content)
        
        #Create new album file with transformed data
        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now())+ ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = album_key, Body = album_content)
        
        #Create new artist file with transformed data
        artist_key = f"transformed_data/artist_data/artist_transformed_{datetime.now()}.csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer)
        artist_content = artist_buffer.getvalue()
        artist_key = artist_key.split('/')[-1]
        s3.Object(Bucket, artist_key).put(Body=artist_content)

        
        
        
        
                        
