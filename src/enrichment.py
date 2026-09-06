from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_rates_df(spark: SparkSession, exchange_rate: DataFrame) -> DataFrame:
    "Transforme la colonne 'rates' (une seule ligne avec un champ par devise) en table (currency, taux)"

    # exchange_rate n'a qu'une seule ligne : on la récupère et on prend le champ "rates"
    rates_row = exchange_rate.select("rates").first()["rates"]

    # rates_row est un objet Row (ex: Row(USD=1, EUR=0.861, JPY=156.02, ...))
    # .asDict() le transforme en dict python classique {"USD": 1, "EUR": 0.861, ...}
    rates_dict = rates_row.asDict()

    # on transforme le dict en liste de tuples (devise, taux)
    rates_list = [(devise, float(taux)) for devise, taux in rates_dict.items()]

    # on recrée une vraie petite table Spark à partir de cette liste
    rates_df = spark.createDataFrame(rates_list, ["currency", "taux"])

    return rates_df


def add_currency_column(spark, df, customers, country_currency, exchange_rate):
    "Ajoute la devise du client et calcule le sous_total dans sa devise locale"

    # Ajouter le pays du client aux commandes
    df = df.join(
        customers.select("customer_id", "country"),
        on="customer_id",
        how="left"
    )

    # Ajouter la devise correspondant au pays
    df = df.join(
        country_currency.select("country", "currency"),
        on="country",
        how="left"
    )

    # Construire la table des taux de change (devise -> taux)
    rates_df = build_rates_df(spark, exchange_rate)

    # Récupérer le taux correspondant à la devise du client (jointure au lieu d'un F.when codé en dur)
    df = df.join(
        rates_df,
        on="currency",
        how="left"
    )
    df = df.withColumnRenamed("taux", "exchange_rate")

    # Calculer le sous-total dans la devise locale
    df = df.withColumn(
        "sous_total_local",
        F.round(F.col("sous_total") * F.col("exchange_rate"), 2)
    )

    return df