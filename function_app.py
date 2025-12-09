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

        # --- [PERBAIKAN UTAMA DI SINI] ---
        # Ganti SELECT spesifik dengan SELECT * agar caption (dan field lain) terbawa otomatis
        query = "SELECT * FROM c ORDER BY c.uploadTime DESC"
        # ---------------------------------
        
        items = list(cosmos_container.query_items(
            query=query, 
            enable_cross_partition_query=True,
            max_item_count=50 
        ))
        
        video_list = []
        storage_account_name = BLOB_CONN_STRING.split("AccountName=")[1].split(";")[0]

        for item in items:
            file_name = item.get("fileName")
            video_url = f"https://{storage_account_name}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{file_name}"
            
            # Prioritaskan username jika ada, kalau tidak pakai userId
            uploader_display = item.get("username") or item.get("userId")

            video_list.append({
                "id": item.get("id"),
                "uploaderId": uploader_display, # Update agar menampilkan username, bukan ID aneh
                "fileName": file_name,
                "blobUrl": video_url,
                "likes": item.get("likes", 0),
                "uploadTime": item.get("uploadTime"),
                
                # Sekarang ini akan berfungsi karena query-nya sudah SELECT *
                "caption": item.get('caption', '') 
            })

        logging.info(f"Berhasil mengambil {len(video_list)} metadata video.")
        
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