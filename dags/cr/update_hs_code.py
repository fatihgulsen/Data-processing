from pyspark.sql import SparkSession
from Spark.CostaRica import CostaRica


def main(table_name):
    costa_rica = CostaRica(table_name=table_name, appName=f"Update HS Code - {table_name}")
    costa_rica.update_hs_code()


if __name__ == "__main__":
    import sys

    table_name = sys.argv[1]
    main(table_name)
