from utils import connection_azure
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import os

load_dotenv()

#   
#   variable utils
#
LOCAL_DIR = "/home/jovyan/data/tmp/"
CONTAINER_NAME = os.environ["AZURE_CONTAINER_RAW"]

#   Liste des fichiers CSV
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



#   Read csv with spark
def read_csv_with_spark(spark, local_dir, file_name):
    "lecture des fichiers CSV depuis le répertoire local et retourne un dict DataFrame"
    dataframes = {}
    
    # Accepte un fichier unique ou une liste
    if isinstance(file_name, str):
        file_name = [file_name]
    for fname in file_name:
        path = os.path.join(local_dir,fname)
        #On enleve le .csv pour avoir le nom de la table
        table_name =  os.path.splitext(fname)[0]
        df = spark.read.csv(path,header=True, inferSchema=True)
        dataframes[table_name] = df
        print(f"Table {table_name} chargée avec {df.count()} lignes")
    return dataframes





#   Session spark
spark = (
    SparkSession.builder
    .appName("ReadCSV")
    .getOrCreate()
)



#   Telecharge tous les CSV et Lecture des CSV avec spark de la liste FILES
def load_all_tables(spark, localpath):
    #   Download files
    download_file_to_local(CONTAINER_NAME, LOCAL_DIR, FILES)
    return read_csv_with_spark(spark, LOCAL_DIR, FILES)

#   Telecharger CSV un fichier Et le lire spécifiquement
def load_specificate_table(spark, filename):
    #   Download files with the name to find in container
    download_file_to_local(CONTAINER_NAME, LOCAL_DIR,filename)
    return read_csv_with_spark(spark, LOCAL_DIR, filename)
