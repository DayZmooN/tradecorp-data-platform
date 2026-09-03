from utils import connection_azure
from dotenv import load_dotenv
import os

load_dotenv()

# Liste des fichiers CSV
FILES = [
    "customers.csv",
    "orders.csv",
    "order_details.csv",
    "products.csv",
    "categories.csv",
    "employees.csv",
    "shippers.csv",
    "suppliers.csv" 
]

def download_file_to_local(container_name, local_dir,file_names):
    """Télécharger les fichiers depuis ADLS vers un repertoire local"""
    client = connection_azure()
    if client is None:
            raise Exception("Impossible de se connecter à ADLS")
    
    container_client = client.get_container_client(container_name)

    # On crée le dossier local s'il n'existe pas
    os.makedirs(local_dir, exist_ok=True)


    for blob in container_client.list_blobs():

        if blob.name in file_names:
            print(f"Téléchargement de {blob.name}")

            blob_client = container_client.get_blob_client(blob.name)
            download_stream = blob_client.download_blob()

            local_path = os.path.join(local_dir,blob.name)

            with open(local_path, "wb") as file:
                file.write(download_stream.readall())

            print(f"{blob.name} téléchargé dans {local_path}")


download_file_to_local(os.environ["AZURE_CONTAINER_RAW"], "/home/jovyan/data/raw",FILES)



