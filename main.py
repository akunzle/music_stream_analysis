from dotenv import load_dotenv
import pandas as pd
import os
import base64
from requests import post, get
import json
import psycopg2
from psycopg2 import sql
import pycountry
from psycopg2.extras import execute_values

load_dotenv()

spotify_client_id = os.getenv("SPOTIFY_CLIENT_ID")
spotify_client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

# Database connection setup using environment variables
def get_db_connection():
    dbname = os.getenv("DB_NAME", "")
    user = os.getenv("DB_USER", "")
    password = os.getenv("DB_PASS", "")
    host = os.getenv("DB_HOST", "")
    return psycopg2.connect(dbname=dbname, user=user, password=password, host=host)

# Function to create tables if they don't already exist
def create_tables():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Playlists (
                    playlist_id VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    description TEXT,
                    followers_total INT,
                    href TEXT,
                    snapshot_id VARCHAR,
                    public BOOLEAN,
                    uri TEXT
                )''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Albums (
                    album_id VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    album_type VARCHAR,
                    release_date DATE,
                    href TEXT,
                    uri TEXT,
                    total_tracks INT,
                    genres TEXT[],
                    label VARCHAR,
                    popularity INT,
                    artist_ids TEXT[]
                )''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Tracks (
                    track_id VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    disc_number INT,
                    track_number INT,
                    duration_ms INT,
                    preview_url TEXT,
                    popularity INT,
                    explicit BOOLEAN,
                    is_local BOOLEAN,
                    href TEXT,
                    uri TEXT,
                    album_id VARCHAR REFERENCES Albums(album_id),
                    playlist_id VARCHAR REFERENCES Playlists(playlist_id)
                )''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Artists (
                    artist_id VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    href TEXT,
                    uri TEXT,
                    genres TEXT[]
                )''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Markets (
                    market_id VARCHAR PRIMARY KEY,
                    name VARCHAR
                )''')
            conn.commit()

# Function to insert playlist data into the database
def insert_playlist_data(playlist):
    sql = """
    INSERT INTO Playlists (playlist_id, name, description, followers_total, href, snapshot_id, public, uri)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (playlist_id) DO UPDATE SET
        name = EXCLUDED.name,
        description = EXCLUDED.description,
        followers_total = EXCLUDED.followers_total,
        href = EXCLUDED.href,
        snapshot_id = EXCLUDED.snapshot_id,
        public = EXCLUDED.public,
        uri = EXCLUDED.uri;
    """
    data = (
        playlist['id'], playlist['name'], playlist.get('description', ''),
        playlist['followers']['total'], playlist['href'], playlist['snapshot_id'],
        playlist['public'], playlist['uri']
    )
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, data)
            conn.commit()

# Function to insert album data into the database
def insert_album_data(album, album_details):
    sql = """
    INSERT INTO Albums (album_id, name, album_type, release_date, href, uri, total_tracks, genres, label, popularity, artist_ids)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (album_id) DO UPDATE SET
        name = EXCLUDED.name,
        album_type = EXCLUDED.album_type,
        release_date = EXCLUDED.release_date,
        href = EXCLUDED.href,
        uri = EXCLUDED.uri,
        total_tracks = EXCLUDED.total_tracks,
        genres = EXCLUDED.genres,
        label = EXCLUDED.label,
        popularity = EXCLUDED.popularity,
        artist_ids = EXCLUDED.artist_ids;  -- Ensure this line is correct based on your schema
    """
    data = (
        album['id'], album['name'], album['album_type'], album['release_date'],
        album['href'], album['uri'], album['total_tracks'],
        album_details['genres'], album_details['label'], album_details['popularity'],
        album_details['artists']  # Pass the list of artist IDs here
    )
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, data)
            conn.commit()

# Function to insert track data into the database
def insert_track_data(track, album_id, playlist_id):
    sql = """
    INSERT INTO Tracks (track_id, name, disc_number, track_number, duration_ms, preview_url, popularity, explicit, is_local, href, uri, album_id, playlist_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (track_id) DO UPDATE SET
        name = EXCLUDED.name,
        disc_number = EXCLUDED.disc_number,
        track_number = EXCLUDED.track_number,
        duration_ms = EXCLUDED.duration_ms,
        preview_url = EXCLUDED.preview_url,
        popularity = EXCLUDED.popularity,
        explicit = EXCLUDED.explicit,
        is_local = EXCLUDED.is_local,
        href = EXCLUDED.href,
        uri = EXCLUDED.uri,
        album_id = EXCLUDED.album_id,
        playlist_id = EXCLUDED.playlist_id;
    """
    data = (
        track['id'], track['name'], track['disc_number'], track['track_number'],
        track['duration_ms'], track.get('preview_url', None), track['popularity'],
        track['explicit'], track['is_local'], track['href'], track['uri'], 
        album_id, playlist_id
    )
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, data)
            conn.commit()

# Function to insert artist data into the database
def insert_artist_data(artist, genres):
    sql = """
    INSERT INTO Artists (artist_id, name, href, uri, genres)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO UPDATE SET
        name = EXCLUDED.name,
        href = EXCLUDED.href,
        uri = EXCLUDED.uri,
        genres = EXCLUDED.genres;  -- Update genres as well
    """
    data = (
        artist['id'], artist['name'], artist['href'], artist['uri'], genres
    )
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, data)
            conn.commit()

def insert_markets(market_ids):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            for market_id in market_ids:
                # Check if the market ID is 'XK' and set the name to 'Kosovo'
                # Otherwise, use pycountry to find the corresponding country name
                if market_id == 'XK':
                    country_name = 'Kosovo'
                else:
                    country = pycountry.countries.get(alpha_2=market_id)
                    country_name = country.name if country else "Unknown"

                # Check if the entry exists and update it; if not, insert the new entry
                sql = """
                INSERT INTO Markets (market_id, name)
                VALUES (%s, %s)
                ON CONFLICT (market_id)
                DO UPDATE SET name = EXCLUDED.name;
                """
                cursor.execute(sql, (market_id, country_name))
            conn.commit()

def get_token():
    auth_string = spotify_client_id + ":" + spotify_client_secret
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    result = post(url, headers=headers, data=data)
    json_result = json.loads(result.content)
    token = json_result["access_token"]
    return token

def get_auth_header(token): 
    return{"Authorization": "Bearer " + token}

def get_playlist_artists(token, playlist_id):
    url = "https://api.spotify.com/v1/playlists/" + playlist_id
    headers = get_auth_header(token)
    result = get(url, headers=headers)
    json_result = json.loads(result.content)
    return json_result

def get_available_markets(token):
    url = "https://api.spotify.com/v1/markets"
    headers = get_auth_header(token)
    result = get(url, headers=headers)
    json_result = json.loads(result.content)["markets"]
    return json_result

def fetch_album_details(token, album_id):
    url = f"https://api.spotify.com/v1/albums/{album_id}"
    headers = get_auth_header(token)
    response = get(url, headers=headers)
    album_details = response.json()
    artist_ids = [artist['id'] for artist in album_details.get('artists', [])]
    return {
        'genres': album_details.get('genres', []),
        'label': album_details.get('label', 'Unknown'),
        'popularity': album_details.get('popularity', 0),
        'artists': artist_ids
    }

def fetch_artist_genres(token, artist_id):
    url = f"https://api.spotify.com/v1/artists/{artist_id}"
    headers = get_auth_header(token)
    response = get(url, headers=headers)
    artist_details = response.json()
    return artist_details.get('genres', [])  # Return genres list

# Execute the query and write to CSV
def query_to_csv():
    conn = get_db_connection()
    query = """
    SELECT 
        Tracks.name AS "Track Name", 
        Tracks.popularity AS "Popularity",
        Albums.name AS "Album Name", 
        Artists.name AS "Artist Name", 
        Albums.label,
        Artists.genres
    FROM 
        Tracks
    JOIN 
        Albums ON Tracks.album_id = Albums.album_id
    JOIN 
        Artists ON Artists.artist_id = ANY(Albums.artist_ids)
    ORDER BY Tracks.popularity DESC;
    """
    df = pd.read_sql_query(query, conn)
    df.to_csv('labels_set.csv', index=False)  # Save to CSV without the DataFrame index

query_to_csv()

def main():
    create_tables()  # Ensure tables are created
    token = get_token()
    playlist = get_playlist_artists(token, "37i9dQZEVXbNG2KDcFcKOF")
    insert_playlist_data(playlist)
    markets = get_available_markets(token)  # This should return a list of market codes
    insert_markets(markets)  # Insert market codes into the database

    # Loop through each track in the playlist
    for item in playlist.get('tracks', {}).get('items', []):
        track = item.get('track')
        if not track:
            continue

        # Insert album data
        album = track.get('album')
        if album:
            album_details = fetch_album_details(token, album['id'])
            insert_album_data(album, album_details)

            # Insert track data
            insert_track_data(track, album['id'], playlist['id'])

        # Insert artist data
        for artist in track.get('artists', []):
            genres = fetch_artist_genres(token, artist['id'])  # Fetch genres
            insert_artist_data(artist, genres)  # Insert artist data with genres

if __name__ == "__main__":
    main()


# # Main execution logic
# create_tables()  # Ensure tables are created
# token = get_token()
# playlist = get_playlist_artists(token, "37i9dQZEVXbNG2KDcFcKOF")
# insert_playlist_data(playlist)