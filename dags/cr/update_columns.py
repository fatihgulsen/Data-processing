import os
import sys

from Spark.CostaRica import CostaRica

os.environ['PYTHONPATH'] = "/opt/airflow/scripts"


def main(table_name):
    costa_rica = CostaRica(table_name=table_name, appName=f"Update Columns - {table_name}")
    costa_rica.update_columns()


if __name__ == "__main__":
    import sys

    table_name = sys.argv[1]
    main(table_name)
