import os
import time
import isodate
from googleapiclient.discovery import build
from pymongo import MongoClient, UpdateOne

YOUTUBE_API_KEY = os.getenv("https://developers.google.com/youtube/v3", "AIzaSyBQNxhayEu3EjsLbdONWGM0n2tKz1-YcJg")
MONGO_URI       = os.getenv("MONGO_URI",       "mongodb://localhost:27017/")
DB_NAME         = "youtube_etl"
COL_VIDEOS      = "youtube_videos"

class YouTubeCategoryClassifier:
    """
    Converte category_id em título de categoria legível.
    """
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

yt_client = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
mongo     = MongoClient(MONGO_URI)
db        = mongo[DB_NAME]
col_videos = db[COL_VIDEOS]

def extract_trending(region: str="BR", max_results: int=10):
    """Busca trending e retorna lista de itens brutos."""
    resp = yt_client.videos().list(
        part="snippet,statistics,contentDetails",
        chart="mostPopular",
        regionCode=region,
        maxResults=max_results
    ).execute()
    return resp.get("items", [])


def transform(items):
    """Filtra campos, converte e adiciona category_title."""
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
            "video_id":      v["id"],
            "Titulo":         sn.get("title"),
            "Canal":       sn.get("channelTitle"),
            "category_id":   sn.get("categoryId"),
            "Visualizações":    int(st.get("viewCount", 0)),
            "Likes":    int(st.get("likeCount", 0)),
            "Comentarios": int(st.get("commentCount", 0)),
            "Duração do video":  duration_sec
        }
        rec = classifier.transform_record(rec)
        records.append(rec)
    return records


def load_videos(records):
    """Upsert dos registros na coleção youtube_videos."""
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
        print(f"[{COL_VIDEOS}] upserted: {result.upserted_count}, modified: {result.modified_count}")


def main():
    regions    = ["BR"]
    max_videos = 10

    for rc in regions:
        print(f"→ Processo para trending {rc}...")
        items = extract_trending(region=rc, max_results=max_videos)
        records = transform(items)
        load_videos(records)
        time.sleep(0.5)

    print("Dados filtrados de YouTube salvos em 'youtube_videos' com sucesso!")

if __name__ == "__main__":
    main()
