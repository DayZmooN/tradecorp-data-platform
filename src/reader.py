from utils import download_file_to_local,download_reference_to_local
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import os

load_dotenv()

#   
#   variable utils
#
LOCAL_DIR = "/home/jovyan/data/"
CONTAINER_RAW = os.environ["AZURE_CONTAINER_RAW"]
REFERENCE_DIR = "reference"

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

RAW_REFERENCE_FILE = [
    "country_currency.csv",
    "exchange_rate.json"
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

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(path)
        )

        dataframes[table_name] = df
        print(f"Table {table_name} chargée avec {df.count()} lignes")
    return dataframes

def read_json_with_spark(spark, local_dir,file_name):
    path = os.path.join(local_dir, file_name)

    table_name = os.path.splitext(file_name)[0]

    df = spark.read.json(path)

    print(f"Table {table_name} chargée avec {df.count()} lignes")

    return {table_name: df}



def read_reference(spark, localpath):
    reference_dir = os.path.join(
        localpath,
        REFERENCE_DIR
    )

    country_currency = read_csv_with_spark(
        spark,
        reference_dir,
        "country_currency.csv"
    )

    exchange_rate = read_json_with_spark(
        spark,
        reference_dir,
        "exchange_rate.json"
    )
    country_currency.update(exchange_rate)

    return country_currency


#   Session spark
spark = (
    SparkSession.builder
    .appName("ReadCSV")
    .getOrCreate()
)



#   Telecharge tous les CSV et Lecture des CSV avec spark de la liste FILES
def load_all_tables(spark, localpath):
    #   RAW tables
    download_file_to_local(CONTAINER_RAW, localpath, FILES)
    raw_dataframes = read_csv_with_spark(
        spark,
        localpath,
        FILES
    )

    #Refernce tables
    reference_dir =  os.path.join(
        localpath,
        REFERENCE_DIR
    )
    download_reference_to_local(
        CONTAINER_RAW,
        reference_dir,
        RAW_REFERENCE_FILE
    )

    #read reference
    reference = read_reference(
        spark,
        localpath
    )
    # merge 
    raw_dataframes.update(reference)


    return raw_dataframes  

#   Telecharger CSV un fichier Et le lire spécifiquement
def load_specificate_table(spark,localpath ,filename):
    #   Download files with the name to find in container
    download_file_to_local(CONTAINER_RAW, localpath,filename)
    return read_csv_with_spark(spark, localpath, filename)



## TEST reader.py 

# data = load_all_tables(
#     spark,
#     os.path.join(LOCAL_DIR, "raw")
# )

# print("Tables chargées :")
# print(data.keys())