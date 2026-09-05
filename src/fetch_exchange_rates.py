import requests
import json
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv
from utils import connection_azure
from pathlib import Path

load_dotenv()


#TODO function upload file in Azure
# def upload_to_adls(container_name, file_path):
#     """Upload file in container"""

#     client = connection_azure()

#     # Nom du fichier local
#     file_name = Path(file_path).name

#     # Chemin dans le container
#     blob_path = f"reference/{file_name}"

#     blob_client = client.get_blob_client(
#         container=container_name,
#         blob=blob_path
#     )

#     with open(file_path, "rb") as file:
#         blob_client.upload_blob(file, overwrite=True)

#     print(
#         f"Le fichier {file_name} a été publié avec succès "
#         f"dans {container_name}/{blob_path}"
#     )

def fetch_and_upload():
    url = "https://api.exchangerate-api.com/v4/latest/USD"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Upload vers ADLS
    conn_str = os.environ["AZURE_STORAGE_URL"]
    client = BlobServiceClient.from_connection_string(conn_str)
    container = os.environ["AZURE_CONTAINER_RAW"]
    blob_name = "reference/exchange_rate.json"
    blob_client = client.get_blob_client(container=container, blob=blob_name)
    blob_client.upload_blob(json.dumps(data), overwrite=True)
    print("Taux de change téléchargé et uploadé avec succés")


if __name__ == "__main__":
    fetch_and_upload()
