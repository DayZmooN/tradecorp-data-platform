from pyspark.sql import SparkSession
from pyspark.sql import Row
import pytest
from src.utils import clean_customers, clean_orders, add_sous_total
from src.enrichment import add_currency_column


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("test").getOrCreate()

def test_clean_orders(spark):
    data = [
        (1, "2023-01-01", "2023-01-10", None, 10.5, 1),
        (2, "2023-01-02", "2023-01-12", "2023-01-11", 12.3, 2),
    ]
    cols = ["order_id", "order_date", "required_date", "shipped_date", "freight", "ship_via"]
    df = spark.createDataFrame(data,cols)
    cleaned = clean_orders(df)
    #on doit recuperer une seule ligne celle avec shipped_date non null
    assert cleaned.count() == 1
    row = cleaned.collect()[0]
    assert row["is_shipped"] == True
    # le renomage
    assert row["shipper_id"] == 2


def test_add_sous_total(spark):
    data = [(1,10,2,0.1)] # order_id prix_unitaire, quantite, discound 
    cols = ["order_detail_id", "prix_unitaire", "quantite", "discount"]
    df = spark.createDataFrame(data, cols)
    result = add_sous_total(df)
    sous_total = result.collect()[0]["sous_total"]
    assert round(sous_total, 2) == 18.0 # Le calcule serat 10*2*(1-0.1) = 18

def test_clean_customers(spark):
    data =   data = [("ALFKI", "  Alfreds Futterkiste ", "  maria anders  ", "  germany ")]
    cols = ["customer_id", "compagny_name", "contact_name", "country"]
    df_customers = spark.createDataFrame(data, cols)
    cleaned = clean_customers(df_customers)
    row = cleaned.collect()[0]
    assert row["compagny_name"] == "Alfreds Futterkiste" # trim
    assert row["contact_name"] == "Maria Anders" # initcap
    assert row["country"] == "GERMANY" # upper



def test_add_currency_column(spark):
    # commande avec juste ce qu'il faut pour le test (customer_id + sous_total)
    data_orders = [("C1", 100.0)]
    cols_orders = ["customer_id", "sous_total"]
    df = spark.createDataFrame(data_orders, cols_orders)

    # client
    data_customers = [("C1", "FRANCE")]
    cols_customers = ["customer_id", "country"]
    customers = spark.createDataFrame(data_customers, cols_customers)

    # mapping pays -> devise
    data_country_currency = [("FRANCE", "EUR")]
    cols_country_currency = ["country", "currency"]
    country_currency = spark.createDataFrame(data_country_currency, cols_country_currency)

    # taux simulé (dict codé en dur, pas d'appel réseau)
    # on reproduit la structure de exchange_rate.json : une colonne "rates" qui est un struct
    rates_data = [Row(rates=Row(EUR=0.9, USD=1.0))]
    exchange_rate = spark.createDataFrame(rates_data)

    result = add_currency_column(spark, df, customers, country_currency, exchange_rate)
    row = result.collect()[0]

    assert row["currency"] == "EUR"
    assert row["exchange_rate"] == 0.9
    # le calcul sera 100 * 0.9 = 90.0
    assert round(row["sous_total_local"], 2) == 90.0
