import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient, PartitionKey
import uuid
import logging
import json
import datetime

app = func.FunctionApp()

# Konfigurasi connection string Blob Storage (gunakan Application Settings di Azure)
BLOB_CONN_STRING = "DefaultEndpointsProtocol=https;AccountName=uploadvidservicefunc123;AccountKey=gKt+BNW0iCObVQT7al9DfjKVqRgiCzC78c9zRBWfVg8hrPndGIRibwQl8pkINrrl1+Ts25lxtRFI+ASto3f3YQ==;EndpointSuffix=core.windows.net"
BLOB_CONTAINER_NAME = "videos"

# KONFIGURASI COSMOS DB
COSMOS_DB_ENDPOINT = "https://tiktok.documents.azure.com:443/"
COSMOS_DB_KEY = "OYNwhYosf6V4QaDxBIjgm2FkZXw53W0pErxYJyMKVZEGhXsdYhNLeOWvvq77DiWqpgu0uc4KrzPiACDb3WfdwQ=="
COSMOS_DB_DATABASE_NAME = "VideoMetadataDB"
COSMOS_DB_CONTAINER_NAME = "Videos"

def get_cosmos_client():
    client = CosmosClient(COSMOS_DB_ENDPOINT, COSMOS_DB_KEY)
    database = client.get_database_client(COSMOS_DB_DATABASE_NAME)
    container = database.get_container_client(COSMOS_DB_CONTAINER_NAME)
    return container

@app.route(route="video/{file_name}", methods=["GET"])
def get_video(req: func.HttpRequest) -> func.HttpResponse:
    file_name = req.route_params.get("file_name")

    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STRING)
    blob_client = blob_service.get_blob_client("videos", file_name)

    try:
        stream = blob_client.download_blob()
        data = stream.readall()
        
        return func.HttpResponse(
            body=data,
            mimetype="video/mp4",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(str(e), status_code=404)

@app.route(route="videos", methods=["GET"])
def list_videos(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("Memulai pengambilan daftar video dari Cosmos DB...")
        
        cosmos_container = get_cosmos_client()

        # Kita ambil SEMUA data metadata yang relevan untuk FEEDS
        # Termasuk fileName, userId, dan likes
        query = "SELECT c.id, c.fileName, c.userId, c.likes, c.uploadTime FROM c ORDER BY c.uploadTime DESC"
        
        # Eksekusi kueri
        # Gunakan max_item_count untuk performa jika data sudah banyak
        items = list(cosmos_container.query_items(
            query=query, 
            enable_cross_partition_query=True,
            # Ambil 50 item teratas saja (untuk feed)
            max_item_count=50 
        ))
        
        # Mengubah hasil kueri menjadi daftar objek
        video_list = []
        storage_account_name = BLOB_CONN_STRING.split("AccountName=")[1].split(";")[0]

        for item in items:
            # Menggabungkan data Cosmos DB dengan URL Blob Storage
            file_name = item.get("fileName")
            video_url = f"https://{storage_account_name}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{file_name}"
            
            video_list.append({
                "id": item.get("id"),
                "uploaderId": item.get("userId"),
                "fileName": file_name,
                "blobUrl": video_url, # URL penuh untuk tag <video>
                "likes": item.get("likes", 0), # Default 0 jika tidak ada
                "uploadTime": item.get("uploadTime"),
                "caption": f"Ini adalah video dari user {item.get('userId')}" # Contoh Caption Sederhana
                # Tambahkan properti lain (misal: user_profile_pic, music_title) di sini
            })

        logging.info(f"Berhasil mengambil {len(video_list)} metadata video.")
        
        # Mengembalikan daftar objek metadata (termasuk URL)
        return func.HttpResponse(
            body=json.dumps({"videos": video_list}),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error fetching videos from Cosmos DB: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Error fetching videos: {str(e)}"}),
            mimetype="application/json",
            status_code=500
        )
