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
def clean_customers(df):
    "Nettoie la table customers"
    #suppression de espace de toutes les colonnes string
    for col in df.columns:
        if df.schema[col].dataType.typeName() == "string":
            df = df.withColumn(col,F.trim(F.col(col)))

    #Mise en forme contact_name
    df = df.withColumn("contact_name", F.initcap("contact_name"))
    #pays en majuscules
    df = df.withColumn("country", F.upper("country"))
    #suppression des doublons sur customer_id
    df = df.dropDuplicates(["customer_id"])
    return df

def clean_orders(df):
    "Netoie la table orders"
    df = df.na.drop(subset=["shipped_date"])
    #conversion en date
    col_date = ["order_date","required_date","shipped_date"]
    for cd in col_date:
        df= df.withColumn(cd,F.col(cd).cast(DateType()))

    # freight en double 
    df = df.withColumn("freight",F.col("freight").cast(DoubleType()))
    # Rename ship_via en shipper_id
    df = df.withColumnRename("ship_via","shipper_id")
    # True si shipped_date et non null
    df = df.withColumn("is_shipped", F.col("shipped_date").isNotNull())
    return df

def clean_order_details(df):
    "Nettoie la table order_details"
    #conversion de type
    df = df.withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
    df = df.withColumn("quantity", F.col("quantity").cast(IntegerType()))
    df = df.withColumn("discount", F.col("discount").cast(DoubleType()))

    #Rename colonnes
    df = df.withColumnRenamed("unit_price", "prix_unitaire")
    df = df.withColumnRenamed("quantity","quantite")
    return df

def add_sous_total(df):
    "Ajoute la colonne sous_total"
    df=  df.withColumn("sous_total", F.round(
        F.col("prix_unitaire") * F.col("quantite") * (1 - F.col("discount")),
        2
    ))
    return df

def clean_employees(df):
    "Nettoie la table employees"
    # Sélection des colonnes
    df = df.serlect(
        "employee_id","first_name","last_name","title",
        "hire_date","city","country"
    )

    # ajout de la colonne full_name gestion des null
    df = df.withColumn('full_name', F.concat_ws(" ", F.col("first_name"),F.col("last_name")))

    return df

def clean_product(df):
    "Nettoie la table products"
    #unite_price en double
    df = df.withColumn("unite_price", F.col("unite_price").cast(DoubleType()))
    #en_stock
    df = df.withColumn("en_stock", F.col("units_in_stock") > 0)
    return df