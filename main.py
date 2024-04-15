from dotenv import load_dotenv
import os
import base64
from requests import post, get
import json
import psycopg2
from psycopg2 import sql
import pycountry

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
            # Ensure Albums table is created before Tracks
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Artists (
                    artist_id VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    popularity INTEGER,
                    followers INTEGER,
                    genres TEXT[]
                )''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Albums (
                    album_id VARCHAR PRIMARY KEY,
                    artist_id VARCHAR REFERENCES Artists(artist_id),
                    name VARCHAR,
                    release_date DATE,
                    genres TEXT[],
                    label VARCHAR,
                    popularity INTEGER
                )''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Tracks (
                    song_id VARCHAR PRIMARY KEY,
                    artist_id VARCHAR REFERENCES Artists(artist_id),
                    name VARCHAR,
                    popularity INTEGER,
                    album_id VARCHAR REFERENCES Albums(album_id),
                    release_date DATE
                )''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Markets (
                    market_id VARCHAR PRIMARY KEY,
                    name VARCHAR
                )''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Genres (
                    genre_seed VARCHAR PRIMARY KEY
                )''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ArtistMarketPerformance (
                    artist_id VARCHAR REFERENCES Artists(artist_id),
                    market_id VARCHAR REFERENCES Markets(market_id),
                    stream_counts INTEGER,
                    follower_count_change INTEGER,
                    PRIMARY KEY (artist_id, market_id)
                )''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS EventImpact (
                    event_id SERIAL PRIMARY KEY,
                    artist_id VARCHAR REFERENCES Artists(artist_id),
                    event_type VARCHAR,
                    event_date DATE,
                    location VARCHAR,
                    market_id VARCHAR REFERENCES Markets(market_id),
                    before_metrics TEXT,
                    after_metrics TEXT
                )''')
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

def search_for_artist(token, artist_name): 
    url = "https://api.spotify.com/v1/search"
    headers = get_auth_header(token)
    query = f"?q={artist_name}&type=artist&limit=1"
    
    query_url = url + query
    result = get(query_url, headers=headers)
    json_result = json.loads(result.content)["artists"]["items"]
    if len(json_result) == 0:
        print("No artist with this name exists...")
        return None
    
    return json_result[0]

def get_songs_by_artist(token, artist_id):
    url = "https://api.spotify.com/v1/artists/" + artist_id + "/top-tracks"
    headers = get_auth_header(token)
    result = get(url, headers=headers)
    json_result = json.loads(result.content)["tracks"]
    return json_result

def get_available_markets(token):
    url = "https://api.spotify.com/v1/markets"
    headers = get_auth_header(token)
    result = get(url, headers=headers)
    json_result = json.loads(result.content)["markets"]
    return json_result

def get_available_genres(token):
    url = "https://api.spotify.com/v1/recommendations/available-genre-seeds"
    headers = get_auth_header(token)
    result = get(url, headers=headers)
    json_result = json.loads(result.content)["genres"]
    return json_result

def insert_genres(genres):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            for genre in genres:
                sql = """
                INSERT INTO Genres (genre_seed)
                VALUES (%s)
                ON CONFLICT (genre_seed) DO NOTHING;
                """
                cursor.execute(sql, (genre,))
            conn.commit()


def insert_artist(artist):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Debugging data types
            print(f"ID: {artist['id']}, Name: {artist['name']}, Popularity: {artist['popularity']}, Followers: {artist['followers']}, Genres: {artist['genres']}")
            print(f"Types - ID: {type(artist['id'])}, Name: {type(artist['name'])}, Popularity: {type(artist['popularity'])}, Followers: {type(artist['followers'])}, Genres: {type(artist['genres'])}")

            # Assuming 'followers' might be a dictionary
            followers = artist['followers'] if isinstance(artist['followers'], int) else artist['followers'].get('total', 0)

            sql = """
            INSERT INTO Artists (artist_id, name, popularity, followers, genres)
            VALUES (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO UPDATE SET
            name = EXCLUDED.name, popularity = EXCLUDED.popularity, followers = EXCLUDED.followers, genres = EXCLUDED.genres;
            """
            cursor.execute(sql, (artist['id'], artist['name'], artist['popularity'], followers, artist['genres']))
            conn.commit()

def insert_album(album, artist_id):
    if not album:  # If album data is None, skip insertion
        return
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            sql = """
            INSERT INTO Albums (album_id, artist_id, name, release_date, genres, label, popularity)
            VALUES (%s, %s, %s, %s, %s, %s, %s) 
            ON CONFLICT (album_id) DO UPDATE SET
            name = EXCLUDED.name, release_date = EXCLUDED.release_date,
            genres = EXCLUDED.genres, label = EXCLUDED.label, popularity = EXCLUDED.popularity;
            """
            cursor.execute(sql, (album.get('id'), artist_id, album.get('name'), album.get('release_date'), album.get('genres', []),
                album.get('label'), album.get('popularity')))
            conn.commit()

def insert_track(track, artist_id):
    # Ensure album exists in the Albums table before inserting track
    insert_album(track.get('album'), artist_id)  # Insert or update album data
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            sql = """
            INSERT INTO Tracks (song_id, artist_id, name, popularity, album_id, release_date)
            VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (song_id) DO UPDATE SET
            name = EXCLUDED.name, popularity = EXCLUDED.popularity, album_id = EXCLUDED.album_id, release_date = EXCLUDED.release_date;
            """
            release_date = track.get('album', {}).get('release_date', None)
            album_id = track.get('album', {}).get('id', None)
            cursor.execute(sql, (track['id'], artist_id, track['name'], track['popularity'], album_id, release_date))
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

# Main execution logic
def main():
    token = get_token()
    artist_info = search_for_artist(token, "Harry Styles")
    genres = get_available_genres(token)  # This should return a list of genres
    insert_genres(genres)  # Insert genre seeds into the database
    markets = get_available_markets(token)  # This should return a list of market codes
    insert_markets(markets)  # Insert market codes into the database
    if artist_info:
        insert_artist(artist_info)
        artist_id = artist_info["id"]
        songs = get_songs_by_artist(token, artist_id)
        for song in songs:
            insert_track(song, artist_id)
            print(f"{song['name']}")

if __name__ == "__main__":
    create_tables()
    main()