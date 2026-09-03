from utils import connection_azure
from dotenv import load_dotenv
import os

load_dotenv()
client = connection_azure()


def download_file_to_local(client,container_name, local_path):
    """Télécharger les fichiers depuis ADLS vers un repertoire local"""
    container_client = client.get_container_client(container_name)

    
    for blob in container_client.list_blobs():
        print(blob.name)
        blob_client = container_client.get_blob_client(blob.name)
        print(blob_client)
        download = blob_client.download_blob()

        file_path  = os.path.join(local_path, blob.name)
        with open(file_path, "wb") as file:
            
            file.write(download.readall())

        print(f"{blob.name} téléchargé")


download_file_to_local(client, os.environ["AZURE_CONTAINER_RAW"], "/home/jovyan/data")



# lire les fichiers country_currency.csv 