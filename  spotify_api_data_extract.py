import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlist_link = "https://open.spotify.com/playlist/2bcSVCNr1AtyVrB3YP9dLl"
    playlist_URI = playlist_link.split('/')[-1]
    spotify_data = sp.playlist_tracks(playlist_URI)
    print(spotify_data)
    # we want to store this data in S3 bucket
    client = boto3.client('s3')

    filename = "spotify_raw" + str(datetime.now()) + ".json" # added later when I got / in s3 bucket
    client.put_object( 
        Bucket = "spotify-etl-project-monika26",
        Key = "raw_data/to_processed/" + filename,
        Body = json.dumps(spotify_data)
    )

# extraction is done, if you see architecture of project - (1st column of extraction (lambda fun done, store in s3 bucket done))
