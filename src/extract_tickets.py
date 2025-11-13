# src/extract_tickets.py

from pathlib import Path
from azure.storage.blob import ContainerClient
from azure.core.exceptions import HttpResponseError

from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

AZURE_BLOB_URL = os.getenv("AZURE_BLOB_SAS_URL")

if not AZURE_BLOB_URL:
    raise ValueError(
        "Environment variable AZURE_BLOB_SAS_URL is missing. "
        "Please define it in your .env file."
    )

# Bronze layer folder
BRONZE_DIR = Path("data/bronze")
BRONZE_DIR.mkdir(parents=True, exist_ok=True)


def download_support_tickets() -> None:
    """
    Download the support tickets JSONL file from Azure Blob Storage
    using the Azure Blob SDK and store it in the bronze layer.
    """
    print("Connecting to Azure Blob container...")

    try:
        container = ContainerClient.from_container_url(AZURE_BLOB_URL)
    except ValueError as e:
        print("Invalid container SAS URL.")
        print(f"Details: {e}")
        return

    # List blobs
    try:
        blobs = list(container.list_blobs())
    except HttpResponseError as e:
        print("Failed to list blobs in container.")
        print(f"Details: {e}")
        return

    if not blobs:
        print("No blobs found inside the container.")
        return

    # Prefer JSONL
    jsonl_blobs = [b for b in blobs if b.name.lower().endswith(".jsonl")]
    target_blob = jsonl_blobs[0] if jsonl_blobs else blobs[0]

    print(f"Found blob: {target_blob.name}")

    blob_client = container.get_blob_client(target_blob.name)

    output_path = BRONZE_DIR / "support_tickets.jsonl"
    print(f"Downloading blob to {output_path} ...")

    try:
        stream = blob_client.download_blob()
        data = stream.readall()
    except HttpResponseError as e:
        print("Failed to download blob.")
        print(f"Details: {e}")
        return

    with open(output_path, "wb") as f:
        f.write(data)

    print("Download completed.")


if __name__ == "__main__":
    download_support_tickets()
