from pyspark.sql import SparkSession
from utils import connection_azure
import logging
import os
from transformer import apply_cleaning, build_enriched
from enrichment import add_currency_column
from writer import upload_files_adls
from reader import load_all_tables


# chemin docker data
LOCAL_DIR = "/home/jovyan/data/"
LOCAL_DIR_RAW = f"{LOCAL_DIR}/raw"
LOCAL_DIR_RAW_REFERENCE = f"{LOCAL_DIR}/raw/reference"
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

    try:
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

        # ajout de la devise et du sous_total_local
        df_enriched = add_currency_column(
            spark,
            df_enriched,
            df_cleaned["customers"],
            dataframes["country_currency"],
            dataframes["exchange_rate"]
        )
        logger.info("Devise et sous_total_local ajoutés")

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

        df_enriched.show(1)
        df_enriched.printSchema()
    except Exception as e:
        logger.error(f"Le pipeline a échoué : {e}")
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("SparkSession arrêtée")


pipline()