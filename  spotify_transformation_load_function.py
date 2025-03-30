import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_external_URLs = row['track']['album']['external_urls']
        album_info = {'album_id':album_id, 'album_name':album_name, 'album_release_date':album_release_date, 'album_total_tracks':album_total_tracks,'album_external_URLs':album_external_URLs}
        album_list.append(album_info)
    return album_list


def artists(data):
    artist_list = []
    for i in data['items']:
        for key,value in i.items():
            if(key == 'track'):
                for artist in value['artists']:
                    artist_info = {'artist_id':artist['id'],'artist_name':artist['name'],'external_urls':artist['href']}
                    artist_list.append(artist_info)
    return artist_list


def songs(data):
    songs_list = []
    for row in data['items']:
        song_added_at = row['added_at']
        song_duration_ms = row['track']['duration_ms']
        song_external_urls = row['track']['external_urls']['spotify']
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_popularity = row['track']['popularity']
        song_album_id = row['track']['album']['id']
        song_artist_id = row['track']['artists'][0]['id']
        song_info = {'song_id':song_id,'song_name':song_name,'song_popularity':song_popularity,'song_duration_ms':song_duration_ms,
                    'song_added_at':song_added_at,'song_external_urls':song_external_urls, 'song_album_id':song_album_id,'song_artist_id':song_artist_id   }
        songs_list.append(song_info)
    return songs_list


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "spotify-etl-project-monika26"
    Key = "raw_data/to_processed/"

    spotify_data = []
    spotify_keys = []
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        response = s3.get_object(Bucket=Bucket, Key=file_key)
        content = response['Body']
        jsonObject = json.loads(content.read())
        spotify_data.append(jsonObject)
        spotify_keys.append(file_key)

    for data in spotify_data:
        album_list = album(data)
        artist_list = artists(data)
        songs_list = songs(data)
    
        album_df = pd.DataFrame(album_list)
        album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'], errors = 'coerce')
        album_df = album_df.drop_duplicates(subset=['album_id'])

        artist_df = pd.DataFrame(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])

        songs_df = pd.DataFrame(songs_list)
        songs_df['song_added_at'] = pd.to_datetime(songs_df['song_added_at'], errors = 'coerce')
        songs_df = songs_df.drop_duplicates(subset=['song_id'])

        album_key = "transformed_data/album_data/album_transformed_data" + str(datetime.now()) + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index = False) 
        #index False becoz, glue crawler will not be able to read 1 additional column of index(0,1,2,3,4....) so we don't make indexes
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = album_key, Body = album_content)

        artist_key = "transformed_data/artists_data/artist_transformed_data" + str(datetime.now()) + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index = False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key = artist_key, Body = artist_content)

        song_key = "transformed_data/songs_data/song_transformed_data" + str(datetime.now()) + ".csv"
        song_buffer = StringIO()
        songs_df.to_csv(song_buffer, index = False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key = song_key, Body = song_content)
    
    # we will copy data from to_processed to processed folder 

    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        source_bkt = {
            'Bucket' : Bucket,
            'Key' : key
        }
        s3_resource.meta.client.copy(source_bkt, Bucket, 'raw_data/processed/' + key.split("/")[-1]) 
        s3_resource.Object(Bucket,key).delete() 




        