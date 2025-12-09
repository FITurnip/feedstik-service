import azure.functions as func
from azure.storage.blob import BlobServiceClient # type: ignore
import uuid
import logging

app = func.FunctionApp()

# Konfigurasi connection string Blob Storage (gunakan Application Settings di Azure)
BLOB_CONN_STRING = "DefaultEndpointsProtocol=https;AccountName=uploadvidservicefunc123;AccountKey=gKt+BNW0iCObVQT7al9DfjKVqRgiCzC78c9zRBWfVg8hrPndGIRibwQl8pkINrrl1+Ts25lxtRFI+ASto3f3YQ==;EndpointSuffix=core.windows.net"
BLOB_CONTAINER_NAME = "videos"


@app.route(route="uploadVideo", auth_level=func.AuthLevel.FUNCTION, methods=["POST"])
def upload_video(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("Upload video request diterima...")

        # Ambil file video dari request
        file = req.files.get('video')

        if not file:
            return func.HttpResponse(
                "Video file is required (field name: video)",
                status_code=400
            )

        # Validasi MIME type sederhana
        allowed_types = ["video/mp4", "video/webm", "video/ogg"]
        if file.content_type not in allowed_types:
            return func.HttpResponse(
                f"Tipe file tidak diizinkan: {file.content_type}",
                status_code=400
            )

        # Generate nama file unik
        ext = file.filename.split(".")[-1]
        file_name = f"{uuid.uuid4()}.{ext}"

        # Upload ke Blob Storage
        blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STRING)
        container_client = blob_service.get_container_client(BLOB_CONTAINER_NAME)

        container_client.upload_blob(
            name=file_name,
            data=file.stream,
            overwrite=False
        )

        video_url = f"https://YOUR_STORAGE_ACCOUNT.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{file_name}"

        logging.info(f"Upload sukses: {video_url}")

        # Return URL
        return func.HttpResponse(
            body=f'{{"message":"Upload success","url":"{video_url}"}}',
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.error(f"Error saat upload: {e}")
        return func.HttpResponse(
            f"Internal Server Error: {str(e)}",
            status_code=500
        )

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

        video_urls = [
            f"https://uploadvidservicefunc123.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{blob.name}"
            for blob in blobs
        ]

        return func.HttpResponse(
            body=str(video_urls),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        return func.HttpResponse(
            f"Error fetching videos: {str(e)}",
            status_code=500
        )
