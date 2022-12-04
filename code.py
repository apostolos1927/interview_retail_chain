import os
import logging
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, to_date, desc, row_number

spark = SparkSession.builder.appName("test").getOrCreate()


def load_csv(filepath):

    try:
        df = spark.read.option("header", True).csv(filepath)
    except Exception as e:
        filename = os.path.split(filepath)[1]
        raise IOError(f"Could not load file {filename}")
    else:
        logging.info("Csv successfully loaded")
        return df


def load_json(filepath):

    try:
        df = spark.read.json(filepath)

    except Exception as e:
        raise IOError("Json failed to load. Json is {}".format(filepath))
    else:
        df = df.withColumn("basket", explode(df.basket))
        # df.printSchema()
        # df2 = df.select(
        #     df.customer_id, df.date_of_purchase, df.basket.price, df.basket.product_id
        # )
        # df2 = df2.withColumnRenamed("basket.price", "price")
        # df2 = df2.withColumnRenamed("basket.product_id", "product_id")
        # df2.show()
        df = df.rdd.map(
            lambda x: (
                x.customer_id,
                x.date_of_purchase,
                x.basket.price,
                x.basket.product_id,
            )
        ).toDF(["customer_id", "date_of_purchase", "price", "product_id"])
    return df


def merge_dataframes(*args):
    df = (
        args[2]
        .alias("trans")
        .join(
            args[0].alias("cust"), args[2].customer_id == args[0].customer_id, "inner"
        )
    ).select("trans.*", "cust.loyalty_score")

    df = (
        df.alias("new_table")
        .join(args[1].alias("product"), df.product_id == args[1].product_id, "inner")
        .select(
            "new_table.*", "product.product_description", "product.product_category"
        )
    )
    return df


def query1(df):
    df.filter((df.product_category == "house") & (df.price >= 400)).select(
        "product_description"
    ).drop_duplicates().show()


def query2(df):
    df.filter(
        (df.product_category.isin("house", "bws", "fruit_veg"))
        & (to_date(df.date_of_purchase).between("2022-07-24", "2022-07-25"))
    ).groupBy("customer_id").count().sort(desc("count")).show()


def query3(df):
    windowSpec = Window.partitionBy("product_id").orderBy(desc("loyalty_score"))
    df = df.withColumn("row_number", row_number().over(windowSpec))
    df.filter(df.row_number == 1).select(
        "product_description",
        "customer_id",
    ).show()


def main(paths):

    df_customers = load_csv(paths[0])
    df_products = load_csv(paths[1])
    df_transactions = load_json(paths[2])
    df_merged = merge_dataframes(df_customers, df_products, df_transactions)
    query1(df_merged)
    query2(df_merged)
    query3(df_merged)


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    paths = [
        "...../customers.csv",
        "...../products.csv",
        "...../transactions",
    ]
    main(paths)
