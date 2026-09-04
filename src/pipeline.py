from pyspark.sql import SparkSession
from utils import connection_azure
import logging
import os
from transformer import apply_cleaning, build_enriched
from writer import upload_files_adls
from reader import load_all_tables


# chemin docker data
LOCAL_DIR = "/home/jovyan/data/"
LOCAL_DIR_RAW = f"{LOCAL_DIR}/raw"
CLEAN_DIR = f"{LOCAL_DIR}/clean"
CONTAINER_CLEAN = os.environ["AZURE_CONTAINER_CLEAN"]

client = connection_azure()


# Configuration du logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

def pipline():
    spark = None

    # Initialiser session 
    spark = SparkSession.builder \
            .appName("TradeCorpPipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()


    # Configurer les credentials pour ADLS (si besoin)
    spark.conf.set("fs.azure.account.key.{}.blob.core.windows.net".format(os.environ["AZURE_STORAGE_ACCOUNT"]), 
                   os.environ["AZURE_STORAGE_KEY"])
    logger.info("SparkSession créée")

    # telechargement and lecture 
    dataframes = load_all_tables(spark, LOCAL_DIR_RAW)
    logger.info("Fichier telecharger et chargées")

    # nettoyer et jointure
    df_cleaned = apply_cleaning(dataframes)
    logger.info("données nettoyer")
    # données enrichie
    df_enriched = build_enriched(df_cleaned)
    logger.info("DataFrame enrichi construit")


    # upload des fichier enriched
    upload_files_adls(
        client,
        df_enriched,
        f"{CLEAN_DIR}/orders",
        CONTAINER_CLEAN,
        "enriched/orders",
        "order_enriched.parquet"
    )
    logger.info("Création des fichier en local et upload de fichier dans azure")


pipline()

