from pyspark.sql import SparkSession
from Spark.CostaRica import CostaRica


def main(table_name):
    costa_rica = CostaRica(table_name=table_name, appName=f"Update Country - {table_name}")
    costa_rica.update_columns()
    costa_rica.update_country()
    costa_rica.update_hs_code()
    costa_rica.update_quantity()
    costa_rica.update_ta_codes()
    results = costa_rica.comprehensive_checks()
    print(results)

if __name__ == "__main__":
    import sys

    table_name = sys.argv[1]
    main(table_name)
