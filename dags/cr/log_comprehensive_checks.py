from pyspark.sql import SparkSession
from Spark.CostaRica import CostaRica


def main(table_name):
    costa_rica = CostaRica(table_name=table_name, appName="CostaRicaDataProcessing")
    results = costa_rica.comprehensive_checks()
    print(results)


if __name__ == "__main__":
    import sys

    table_name = sys.argv[1]
    main(table_name)
