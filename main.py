import os
import time
import isodate
from googleapiclient.discovery import build
from pymongo import MongoClient, UpdateOne
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

YOUTUBE_API_KEY        = os.getenv("YOUTUBE_API_KEY", "AIzaSyBQNxhayEu3EjsLbdONWGM0n2tKz1-YcJg")
SPOTIFY_CLIENT_ID      = os.getenv("SPOTIFY_CLIENT_ID", "4c4fd8ba1b7a462e9fb095081d310a82")
SPOTIFY_CLIENT_SECRET  = os.getenv("SPOTIFY_CLIENT_SECRET", "9324df95c66b4b7e80194e1db4231679")
MONGO_URI              = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

mongo = MongoClient(MONGO_URI)
db    = mongo["pipeline_etl"]

yt_client = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
col_videos = db["youtube_videos"]

class YouTubeCategoryClassifier:
    def __init__(self):
        self.id_to_title = {
            "1":  "Film & Animation",
            "2":  "Autos & Vehicles",
            "10": "Music",
            "15": "Pets & Animals",
            "17": "Sports",
            "20": "Gaming",
            "21": "Videoblogging",
            "22": "People & Blogs",
            "23": "Comedy",
            "24": "Entertainment",
            "25": "News & Politics",
            "26": "Howto & Style",
            "27": "Education",
            "28": "Science & Technology",
            "29": "Nonprofits & Activism",
            "30": "Movies",
            "31": "Anime/Animation",
            "32": "Action/Adventure",
            "33": "Classics",
            "34": "Comedy",
            "35": "Documentary",
            "36": "Drama",
            "37": "Family",
            "38": "Foreign",
            "39": "Horror",
            "40": "Sci-Fi/Fantasy",
            "41": "Thriller",
            "42": "Shorts",
            "43": "Shows",
            "44": "Trailers"
        }

    def get_title(self, category_id):
        return self.id_to_title.get(str(category_id), "Unknown")

    def transform_record(self, record, src_field="category_id", dst_field="category_title"):
        cid = record.get(src_field)
        record[dst_field] = self.get_title(cid)
        return record

def extract_trending(region="BR", max_results=10):
    resp = yt_client.videos().list(
        part="snippet,statistics,contentDetails",
        chart="mostPopular",
        regionCode=region,
        maxResults=max_results
    ).execute()
    return resp.get("items", [])

def transform_youtube(items):
    classifier = YouTubeCategoryClassifier()
    records = []
    for v in items:
        sn = v.get("snippet", {})
        st = v.get("statistics", {})
        cd = v.get("contentDetails", {})
        duration_iso = cd.get("duration", "PT0S")
        try:
            duration_sec = int(isodate.parse_duration(duration_iso).total_seconds())
        except Exception:
            duration_sec = 0
        rec = {
            "video_id":       v["id"],
            "Titulo":         sn.get("title"),
            "Canal":          sn.get("channelTitle"),
            "category_id":    sn.get("categoryId"),
            "Visualizações":  int(st.get("viewCount", 0)),
            "Likes":          int(st.get("likeCount", 0)),
            "Comentarios":    int(st.get("commentCount", 0)),
            "Duração_seg":    duration_sec
        }
        rec = classifier.transform_record(rec)
        records.append(rec)
    return records

def load_videos(records):
    ops = []
    for doc in records:
        ops.append(
            UpdateOne(
                {"video_id": doc["video_id"]},
                {"$set": doc},
                upsert=True
            )
        )
    if ops:
        result = col_videos.bulk_write(ops)
        print(f"[YouTube] upserted: {result.upserted_count}, modified: {result.modified_count}")

auth_manager = SpotifyClientCredentials(client_id=SPOTIFY_CLIENT_ID, client_secret=SPOTIFY_CLIENT_SECRET)
sp = spotipy.Spotify(auth_manager=auth_manager)
col_tracks = db["spotify_tracks"]

def ms_to_min_sec(ms):
    minutes = ms // 60000
    seconds = (ms % 60000) // 1000
    return f"{minutes}:{seconds:02d}"

def extract_playlist_tracks(playlist_id, limit=20):
    results = sp.playlist_items(playlist_id, limit=limit)
    return results.get("items", [])
    

def transform_spotify(tracks):
    records = []
    for item in tracks:
        track = item.get("track", {})
        if not track:
            continue
        artists = [artist["name"] for artist in track.get("artists", [])]
        duration_ms = track.get("duration_ms")
        duration_formatted = ms_to_min_sec(duration_ms)  # Formatação da duração

        rec = {
            "track_id":       track.get("id"),
            "Nome":           track.get("name"),
            "Artistas":       artists,
            "Album":          track.get("album", {}).get("name"),
            "Popularidade":   track.get("popularity"),
            "Duração":        duration_formatted,  # Use o formato aqui
            "Data_Lancamento": track.get("album", {}).get("release_date"),
            "URL":            track.get("external_urls", {}).get("spotify"),
        }
        records.append(rec)
    return records

def load_tracks(records):
    ops = []
    for doc in records:
        ops.append(
            UpdateOne(
                {"track_id": doc["track_id"]},
                {"$set": doc},
                upsert=True
            )
        )
    if ops:
        result = col_tracks.bulk_write(ops)
        print(f"[Spotify] upserted: {result.upserted_count}, modified: {result.modified_count}")

def main():
    print("→ Extraindo dados do YouTube...")
    regions    = ["BR"]
    max_videos = 10
    for rc in regions:
        items = extract_trending(region=rc, max_results=max_videos)
        records = transform_youtube(items)
        load_videos(records)
        time.sleep(0.5)

    print("→ Extraindo dados do Spotify...")
    playlist_ids = [
        "0VU29I2nq0iKGTOh5jNAKQ"
    ]
    for pid in playlist_ids:
        tracks = extract_playlist_tracks(pid, limit=20)
        records = transform_spotify(tracks)
        load_tracks(records)
        time.sleep(0.5)

    print("ETL concluído para YouTube e Spotify!")

if __name__ == "__main__":
    main()
