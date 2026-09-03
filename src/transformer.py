from utils import (clean_customers,clean_employees,
                   clean_order_details, clean_orders, 
                   clean_products)
from pyspark.sql import functions as F


def apply_cleaning(df):
    "appliquer le nettoyages a chaque table"
    cleaned= {}
    cleaned["customers"] = clean_customers(df["customers"])
    cleaned["orders"] = clean_orders(df["orders"])
    cleaned["order_details"] = clean_order_details(df["order_details"])
    cleaned["employees"] = clean_employees(df["employees"])
    cleaned["products"] = clean_products(df["products"])
    cleaned["categories"] = df["categories"]
    cleaned["shippers"] = df["shippers"]

    return cleaned

# enriched
def build_enriched(cleaned):
    "Jointure sur toutes les tables pour un DataFrame complet"
    od = cleaned["order_details"].alias("od")
    o = cleaned["orders"].alias("o")
    c = cleaned["customers"].alias("c")
    p = cleaned["products"].alias("p")
    cat = cleaned["categories"].alias("cat")
    e = cleaned["employees"].alias("e")
    sh = cleaned["shippers"].alias("sh")

    #jointure en chaine 
    enriched=  od.join(o, od.order_id == o.order_id , "inner")\
                .join(c, o.customer_id == c.customer_id, "inner")\
                .join(p, od.product_id == p.product_id, "inner")\
                .join(cat, p.category_id == cat.category_id, "inner")\
                .join(e, o.employee_id == e.employee_id, "inner")\
                .join(sh, o.shipper_id == sh.shipper_id, "inner")

    #Selection des colonnes utiliser avec le rename pour éviter les doublons

    enriched = enriched.select(
        o.order_id,
        c.customer_id,
        o.employee_id,
        p.product_id,
        o.order_date,
        o.required_date,
        o.shipped_date,
        o.freight,
        o.is_shipped,
        od.prix_unitaire,
        od.quantite,
        od.discount,
        od.sous_total,
        F.concat_ws(
            " ",
            c.contact_name,
            c.company_name
        ).alias("customer_name"),

        c.country.alias("customer_country"),
        c.city.alias("customer_city"),

        p.product_name,
        cat.category_name,
        p.en_stock,

        e.full_name,

        sh.company_name.alias("shipper_name")
    )


    return enriched
        
