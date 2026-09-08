from utils import connection_azure
from dotenv  import load_dotenv
import os

load_dotenv()

client =  connection_azure()
STORAGE_ACCOUNT = os.environ['AZURE_STORAGE_ACCOUNT']

# for airflow
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("writer").getOrCreate()
LOCAL_DIR ="/home/jovyan/data/tmp/tranformer_output"
LOCAL_OUTPUT = "/home/jovyan/data/tmp/clean_output"
CONTAINER_CLEAN = os.environ["AZURE_CONTAINER_CLEAN"]


FILES = [
    "enriched"
]

def files_output(spark ,file, input_dir):
    dfs = {}
    for f in file:
        output_path = os.path.join(input_dir,f)
        dfs[f] = spark.read.parquet(output_path)
    return dfs

dfs = files_output(spark, FILES,LOCAL_DIR)
# Création du fichier Parquet en local puis upload vers Azure
def upload_files_adls(client, df, local_dir, container_name, azure_path, file_name):
    # Crée le parquet en local
    df.coalesce(1).write.mode("overwrite").parquet(local_dir)

    # Cherche le fichier parquet généré par Spark
    for generated_file in os.listdir(local_dir):
        if generated_file.endswith(".parquet"):
            generated_path = os.path.join(local_dir, generated_file)
            local_file_path = os.path.join(local_dir, file_name)

            # Renomme le fichier
            os.rename(generated_path, local_file_path)

            # Upload vers Azure
            blob_client = client.get_blob_client(
                container=container_name,
                blob=f"{azure_path}/{file_name}"
            )

            with open(local_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            print(f"Fichier créé en local : {local_file_path}")
            print(f"Fichier envoyé sur Azure : {azure_path}/{file_name}")


upload_files_adls(client,dfs["enriched"],LOCAL_OUTPUT,CONTAINER_CLEAN, "enriched/orders", "order_enriched.parquet")




