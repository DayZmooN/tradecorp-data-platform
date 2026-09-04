from utils import connection_azure,download_file_to_local
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import os
from transformer import apply_cleaning, build_enriched
from writer import upload_files_adls

load_dotenv()

#   
#   variable utils
#
LOCAL_DIR = "/home/jovyan/data/"
CONTAINER_RAW = os.environ["AZURE_CONTAINER_RAW"]

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
    download_file_to_local(CONTAINER_RAW, LOCAL_DIR, FILES)
    return read_csv_with_spark(spark, LOCAL_DIR, FILES)

#   Telecharger CSV un fichier Et le lire spécifiquement
def load_specificate_table(spark, filename):
    #   Download files with the name to find in container
    download_file_to_local(CONTAINER_RAW, LOCAL_DIR,filename)
    return read_csv_with_spark(spark, LOCAL_DIR, filename)


#
#   test clean and build for transfromers
#
data = read_csv_with_spark(spark, f"{LOCAL_DIR}/tmp", FILES)
df_clean = apply_cleaning(data)
df_build = build_enriched(df_clean)
# df_build.show()
# df_build.printSchema()



# upload file
client = connection_azure()
container_clean = os.environ["AZURE_CONTAINER_CLEAN"]


upload_files_adls(
    client,
    df_build,
    f"{LOCAL_DIR}/clean/order",
    container_clean,
    "enriched/order",
    "order.parquet"
)




