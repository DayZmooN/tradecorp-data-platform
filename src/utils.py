from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType


load_dotenv()

# storage_account_key = os.environ["AZURE_STORAGE_KEY"]
# storage_account_name = os.environ["AZURE_STORAGE_ACCOUNT"]
# container_name = os.environ["AZURE_CONTAINER_RAW"]

# Connexion ADLS 
def connection_azure():
    connection_string = os.environ["AZURE_STORAGE_URL"]
    try:
        client = BlobServiceClient.from_connection_string(connection_string)
        client.get_account_information()
        print("Connexion à ADLS réussie")        
        return client
    except Exception as e:
        print(f"Erreur de connexion à ADLS : {e}")
        return None

#   download file to local
def download_file_to_local(container_name, local_dir, file_names):
    """Télécharger les fichiers depuis ADLS vers un repertoire local"""
    client = connection_azure()
    if client is None:
        raise Exception("Impossible de se connecter à ADLS")
    
    container_client = client.get_container_client(container_name)

    # On crée le dossier local s'il n'existe pas
    os.makedirs(local_dir, exist_ok=True)

    # Accepte un fichier unique ou une liste
    if isinstance(file_names, str):
        file_names = [file_names]

    for blob in container_client.list_blobs():

        if blob.name in file_names:
            print(f"Téléchargement de {blob.name}")

            blob_client = container_client.get_blob_client(blob.name)
            download_stream = blob_client.download_blob()

            local_path = os.path.join(local_dir,blob.name)

            with open(local_path, "wb") as file:
                file.write(download_stream.readall())

            print(f"{blob.name} téléchargé dans {local_path}")



# Nettoyage des tables
# clean_customers(df):
    ...