from utils import connection_azure
from dotenv import load_dotenv
import os

load_dotenv()

# chemin local du fichier fourni (à adapter selon où tu as mis le csv dans le conteneur)
LOCAL_FILE = "/home/jovyan/data/raw/reference/country_currency.csv"

CONTAINER_RAW = os.environ["AZURE_CONTAINER_RAW"]
BLOB_NAME = "reference/country_currency.csv"


def upload_country_currency():
    "Upload unique du mapping pays/devise vers ADLS (raw/reference/)"

    client = connection_azure()
    if client is None:
        raise Exception("Impossible de se connecter à ADLS")

    blob_client = client.get_blob_client(
        container=CONTAINER_RAW,
        blob=BLOB_NAME
    )

    with open(LOCAL_FILE, "rb") as file:
        blob_client.upload_blob(file, overwrite=True)

    print(f"{LOCAL_FILE} uploadé avec succès vers {CONTAINER_RAW}/{BLOB_NAME}")


if __name__ == "__main__":
    upload_country_currency()