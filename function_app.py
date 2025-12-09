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
        blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STRING)
        container_client = blob_service.get_container_client(BLOB_CONTAINER_NAME)

        blobs = container_client.list_blobs()

        # hanya kirim fileName, lebih bersih
        files = [blob.name for blob in blobs]

        return func.HttpResponse(
            body=json.dumps({"videos": files}),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        return func.HttpResponse(
            f"Error fetching videos: {str(e)}",
            status_code=500
        )
