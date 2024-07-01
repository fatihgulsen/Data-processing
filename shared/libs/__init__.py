from config.CountryCodeConfig import CountryCodes
from config.ShipmentFileConfig import ShipmentFileType
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import (
	coalesce,
	col,
	expr,
	length,
	lit,
	regexp_replace,
	trim,
	when,
)
from pyspark.sql.types import (
	DateType,
	FloatType,
	IntegerType,
	StringType,
	StructField,
	StructType,
)


class Spark4DataProc:
	def __init__(self, url, user, password, appName="SQL_to_PySpark") -> None:
		self.url = url
		self.user = user
		self.password = password
		self.driver = "org.postgresql.Driver"

		conf = SparkConf() \
			.setAppName(appName) \
			.setMaster("spark://spark-master:7077") \
			.set("spark.sql.execution.arrow.pyspark.enabled", "true") \
			.set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
			.set("spark.driver.maxResultSize", "32g") \
			.set('spark.rapids.sql.enabled', 'true')

		sc = SparkContext(conf=conf)
		sqlContext = SQLContext(sc)
		self.spark = sqlContext.sparkSession

		# self.spark = SparkSession.builder.appName(appName).config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.24.jar").getOrCreate()

	def __del__(self):
		self.spark.stop()

	def __get_jdbc_options(self, table_name, database_name):
		"""Internal method to get JDBC options for reading/writing tables."""
		return {
			"url": f"jdbc:postgresql://{self.url}/{database_name}",
			"dbtable": table_name,
			"user": self.user,
			"password": self.password,
			"driver": self.driver,
		}

	def read_table(self, table_name, database_name) -> DataFrame:
		"""Read a table from the specified SQL Server database."""
		options = self.__get_jdbc_options(table_name, database_name)
		return self.spark.read.format("jdbc").options(**options).load()

	def write_table(
		self, df: DataFrame, table_name, database_name, mode="overwrite"
	) -> None:
		"""Write a DataFrame to the specified SQL Server database table."""
		options = self.__get_jdbc_options(table_name, database_name)
		df.write.format("jdbc").options(**options).mode(mode).save()

	def rename_columns(self, df, old_new_columns) -> DataFrame:
		"""Rename columns in the DataFrame based on a mapping dictionary."""
		for old_col, new_col in old_new_columns.items():
			df = df.withColumnRenamed(old_col, new_col)
		return df

	def add_columns(self, df: DataFrame, column_name_type_values: dict) -> DataFrame:
		"""Add new columns to the DataFrame if they do not already exist."""
		existing_columns = df.columns

		for column_name, (column_type, column_value) in column_name_type_values.items():
			if column_name not in existing_columns:
				if column_type == "UUID":
					df = df.withColumn(column_name, expr("uuid()"))
				else:
					value_to_insert = lit(None) if column_value == "" else lit(column_value)
					if column_type == "DATE":
						df = df.withColumn(column_name, value_to_insert.cast(column_type))
					elif column_type == "STRING":
						df = df.withColumn(column_name, value_to_insert.cast(column_type))
					elif column_type == "INTEGER":
						df = df.withColumn(column_name, value_to_insert.cast(column_type))
					else:
						df = df.withColumn(column_name, value_to_insert.cast(column_type))
		return df

	def add_columns_by_type(self, df, column_specs=ShipmentFileType.fields):
		"""
        Adds columns to DataFrame based on their specified types if they do not exist.
        """
		for column_name, specs in column_specs.items():
			if column_name not in df.columns:
				data_type = specs["validation_rules"]
				if "float" in data_type:
					df = df.withColumn(column_name, lit(None).cast(FloatType()))
				elif "date" in data_type:
					df = df.withColumn(column_name, lit(None).cast(DateType()))
				elif "integer" in data_type or "short" in data_type:
					df = df.withColumn(column_name, lit(None).cast(IntegerType()))

		return df

	def reorder_and_maintain_columns(self, df, column_specs=ShipmentFileType.fields):
		"""
        Reorders the DataFrame columns based on the specified column numbers in column_specs
        while maintaining any additional columns that are not specified.
        """
		# Create a sorted list of columns based on the 'column_no' provided in column_specs
		sorted_columns = sorted(
			[
				(col_name, specs)
				for col_name, specs in column_specs.items()
				if col_name in df.columns
			],
			key=lambda x: x[1]["column_no"],
		)

		# Extract the column names from the sorted tuple list
		sorted_column_names = [col_name for col_name, _ in sorted_columns]

		# Identify columns not defined in column_specs but present in the DataFrame
		additional_columns = [col for col in df.columns if col not in column_specs]

		# Combine sorted columns with the additional columns
		final_column_order = sorted_column_names + additional_columns

		# Reorder DataFrame columns according to the final column order
		df = df.select(final_column_order)

		return df

	def update_hs_code_description(self, df, country_code):
		"""
        Updates the HS_CODE and HS_CODE_DESCRIPTION columns in the main DataFrame by matching HS_CODE with
        the HS_CODE_DESC from a country-specific HS description table. Also updates HS_CODE with new_HS_CODE if it is not null.
        """
		# Define the table name based on the country code
		table_name = f"{country_code}_hs_desc"

		# Read the HS description table from the specified database
		hs_desc_df = self.read_table(database_name="HS_DESC", table_name=table_name)

		# Join the main DataFrame with the HS description DataFrame on HS_CODE
		updated_df = df.join(hs_desc_df, df["HS_CODE"] == hs_desc_df["HS_CODE"], "left")

		# Update the HS_CODE_DESCRIPTION with HS_CODE_DESC from the joined table
		select_columns = [col("df." + column) for column in df.columns] + [
			coalesce(col("hs.HS_CODE_DESC"), col("df.HS_CODE_DESCRIPTION")).alias(
				"HS_CODE_DESCRIPTION"
			),
			coalesce(col("hs.new_HS_CODE"), col("df.HS_CODE")).alias("HS_CODE"),
		]

		updated_df = updated_df.select(select_columns)
		return updated_df

	def join_and_update_country(
		self, df: DataFrame, column_name: str, country_code_df: DataFrame
	) -> DataFrame:
		"""Update country columns by joining with the country code DataFrame and replacing values if a match is found."""
		# Join the data frame with the country code data frame based on the specified column,
		# but only select the columns from the original DataFrame after the join.
		df = (
			df.alias("df")
			.join(
				country_code_df.alias("codes"),
				col("df." + column_name) == col("codes.country2"),
				"left",
			)
			.select(
				"df.*",  # Select all columns from the original DataFrame
				when(col("codes.code").isNotNull(), col("codes.code"))
				.otherwise(col("df." + column_name))
				.alias(column_name),
			)
		)

		return df

	def update_country_column(self, df: DataFrame, column_name: str) -> DataFrame:
		"""Update a specific country column using the country code DataFrame."""
		country_code_df = self.read_table("CountryCode", "Country_Port_Code")
		return self.join_and_update_country(df, column_name, country_code_df)

	def update_exporter_country(self, df: DataFrame) -> DataFrame:
		"""Update EXPORTER_COUNTRY column."""
		return self.update_country_column(df, "EXPORTER_COUNTRY")

	def update_country_of_origin(self, df: DataFrame) -> DataFrame:
		"""Update COUNTRY_OF_ORIGIN column."""
		return self.update_country_column(df, "COUNTRY_OF_ORIGIN")

	def update_importer_country(self, df: DataFrame) -> DataFrame:
		"""Update IMPORTER_COUNTRY column."""
		return self.update_country_column(df, "IMPORTER_COUNTRY")

	def match_country_columns(self, df: DataFrame) -> DataFrame:
		"""Ensure country columns have valid values, replacing nulls with appropriate values."""
		for col1, col2 in [
			("COUNTRY_OF_ORIGIN", "EXPORTER_COUNTRY"),
			("EXPORTER_COUNTRY", "COUNTRY_OF_ORIGIN"),
		]:
			df = df.withColumn(
				col1, when(col(col1).isNull(), col(col2)).otherwise(col(col1))
			)
		for _col in ["COUNTRY_OF_ORIGIN", "EXPORTER_COUNTRY"]:
			df = df.withColumn(
				_col, when(col(_col).isNull(), "ZZ").otherwise(col(_col))
			)
		return df

	def update_port(
		self, df: DataFrame, join_column: str, update_column: str
	) -> DataFrame:
		"""Update port columns by joining with the country code DataFrame."""
		country_code_uniq_df = self.read_table("Country_Code_Uniq", "Country_Port_Code")
		return df.join(
			country_code_uniq_df,
			df[join_column] == country_code_uniq_df["Alpha-2 code"],
			"left",
		).withColumn(
			update_column,
			when(
				df[update_column].isNull(),
				country_code_uniq_df["English short name (upper/lower case)"],
			).otherwise(df[update_column]),
		)

	def update_port_of_arrival(self, df: DataFrame) -> DataFrame:
		"""Update PORT_OF_ARRIVAL column."""
		return self.update_port(df, "IMPORTER_COUNTRY", "PORT_OF_ARRIVAL")

	def update_port_of_departure(self, df: DataFrame) -> DataFrame:
		"""Update PORT_OF_DEPARTURE column."""
		return self.update_port(df, "EXPORTER_COUNTRY", "PORT_OF_DEPARTURE")

	def update_column_none(self, df: DataFrame, columns: list[str]) -> DataFrame:
		"""Update None columns."""
		for col_name in columns:
			df = df.withColumn(
				col_name, when(col(col_name) == "", None).otherwise(col(col_name))
			)
		return df

	def update_ta_code(
		self, df, country_column, name_column, ta_code_column, condition
	):
		"""Update TA_CODE column for specified country codes from the CountryCodes class."""
		# Use the country codes defined in the CountryCodes class
		country_codes = CountryCodes.COUNTRY_CODES_TWO_CHARS

		# Iterate over each defined country code and perform updates in the original DataFrame
		for country_code in country_codes:
			if (
				country_code
				in df.select(country_column)
				.distinct()
				.rdd.map(lambda r: r[0])
				.collect()
			):
				table_name = f"{country_code}_kod_atama"

				# Read the TA code assignment table for the specific country
				new_codes_df = self.read_table(table_name, "Yeni_veri_kod_atama_uniq")

				# Join with the TA code DataFrame based on the condition and update the TA code column
				df = df.join(new_codes_df, condition, "left").withColumn(
					ta_code_column,
					when(
						col(ta_code_column).isNull(), new_codes_df["TA_CODE"]
					).otherwise(col(ta_code_column)),
				)

		return df

	def update_exporter_ta_code(self, df):
		"""Update EXPORTER_TA_CODE column based on cleaned names and matching country codes."""
		condition = trim(
			regexp_replace(col("EXPORTER_NAME"), "[.,-\"'&/#() +:<>]", "")
		) == regexp_replace(col("COMPANY_ham"), "[.,-\"'&/#() +:<>]", "")
		return self.update_ta_code(
			df, "EXPORTER_COUNTRY", "EXPORTER_NAME", "EXPORTER_TA_CODE", condition
		)

	def update_importer_ta_code(self, df):
		"""Update IMPORTER_TA_CODE column based on TAX ID and name, handling multiple conditions."""
		condition = trim(
			regexp_replace(col("IMPORTER_NAME"), "[.,-\"'&/#() +:<>]", "")
		) == regexp_replace(col("COMPANY_ham"), "[.,-\"'&/#() +:<>]", "")

		return self.update_ta_code(
			df, "IMPORTER_COUNTRY", "IMPORTER_NAME", "IMPORTER_TA_CODE", condition
		)

	def update_quantity_and_unit(self, df, country_code):
		"""
        Updates 'UNIT_OF_QUANTITY' based on a country-specific mapping table and adjusts 'QUANTITY'
        based on the action specified in the mapping table. Only retains columns from the original DataFrame.
        :param df: DataFrame to update
        :param country_code: Country code to determine which mapping table to use
        """
		# Define the table name based on the country code
		table_name = f"{country_code}_unit_of_quantity"

		# Read the unit of quantity mapping table
		unit_mapping_df = self.read_table(table_name, "your_database_name")

		# Join the main DataFrame with the mapping DataFrame on 'UNIT_OF_QUANTITY'
		df = df.alias("df").join(
			unit_mapping_df.alias("map"),
			on=df["UNIT_OF_QUANTITY"] == unit_mapping_df["UNIT_OF_QUANTITY"],
			how="left",
		)

		# Update 'UNIT_OF_QUANTITY' to the new unit and adjust 'QUANTITY' according to the action
		# Selecting only original df columns and applying transformations
		df = df.select(
			*[
				 col("df." + column)
				 for column in df.columns
				 if column not in unit_mapping_df.columns
			 ]
			 + [
				 when(col("map.Yeni_Birim").isNotNull(), col("map.Yeni_Birim"))
				 .otherwise(col("df.UNIT_OF_QUANTITY"))
				 .alias("UNIT_OF_QUANTITY"),
				 when(
					 col("map.Aksiyon").isNotNull(),
					 col("df.QUANTITY") * col("map.Aksiyon"),
				 )
				 .otherwise(col("df.QUANTITY"))
				 .alias("QUANTITY"),
			 ]
		)

		return df

	def check_null_columns(self, df: DataFrame, columns: list[str]) -> dict[str, int]:
		"""Check for null values in specified columns."""
		return {column: df.where(col(column).isNull()).count() for column in columns}

	def check_missing_country(self, df: DataFrame, country_column: str) -> DataFrame:
		"""Check for missing country codes in specified column."""
		country_code_uniq_df = self.read_table(
			"Country_Port_Code.dbo.Country_Code_Uniq", "Country_Port_Code"
		)
		return (
			df.select(country_column)
			.join(
				country_code_uniq_df, df[country_column] == col("Alpha-2 code"), "left"
			)
			.where(col("Alpha-2 code").isNull())
			.distinct()
		)

	def check_quantity_integrity(self, df, country_code):
		"""
        Checks for null values in QUANTITY and QUANTITY_UNIT columns, and lists UNIT_OF_QUANTITY values
        not found in the unit mapping table specific to the country code provided.
        :param df: DataFrame to check
        :param country_code: Country code to determine which mapping table to use
        :return: Dictionary with results of checks
        """
		results = {}

		# Load the unit mapping table based on country code
		table_name = f"{country_code}_unit_of_quantity"
		unit_mapping_df = self.read_table(table_name, "UNIT_OF_QUANTITY")

		# Check for null values in QUANTITY
		results["null_quantity"] = df.where(col("QUANTITY").isNull()).count()

		# Check for null values in QUANTITY_UNIT
		results["null_quantity_unit"] = df.where(col("QUANTITY_UNIT").isNull()).count()

		# Find UNIT_OF_QUANTITY values not found in the unit mapping table
		unique_units_df = df.select("UNIT_OF_QUANTITY").distinct()
		mapping_units_df = unit_mapping_df.select("UNIT_OF_QUANTITY").distinct()
		missing_units = unique_units_df.join(
			mapping_units_df, ["UNIT_OF_QUANTITY"], "left_anti"
		)
		results["missing_units"] = [
			row["UNIT_OF_QUANTITY"] for row in missing_units.collect()
		]  # Convert to list for easier handling

		return results

	def check_ta_codes(self, df, ta_columns):
		"""Check TA codes for specified columns to ensure they follow the specified format '{CountryCode}{8 digits}'"""
		results = {}
		for column in ta_columns:
			# Regex pattern expects a country code followed by exactly 8 digits
			pattern = r"^[A-Z]{2}\d{8}$"
			invalid_ta = df.filter(
				(col(column).isNull()) | (~col(column).rlike(pattern))
			)
			count_invalid_ta = invalid_ta.count()
			if count_invalid_ta > 0:
				results[column] = count_invalid_ta
		return results

	def check_hs_code_integrity(self, df):
		"""Check HS code length, null values, and ensure HS codes are numeric, returning results in a dictionary"""
		results = {}

		# Check for null values in HS_CODE
		results["null_hs_code"] = df.filter(col("HS_CODE").isNull()).count()

		# Check for null values in HS_CODE_DESCRIPTION
		results["null_hs_code_description"] = df.filter(
			col("HS_CODE_DESCRIPTION").isNull()
		).count()

		# Check if HS_CODE consists only of digits and is not empty
		results["invalid_hs_code_format"] = df.filter(
			~col("HS_CODE").rlike("^\d+$")
		).count()

		return results

	def create_schema_p7(self) -> StructType:
		schema = StructType(
			[
				StructField("ARRIVAL_DATE", DateType(), False),
				StructField("BILL_OF_LADING_NO", StringType(), True),
				StructField("DECLARATION_NUMBER", StringType(), True),
				StructField("HS_CODE", StringType(), True),
				StructField("HS_CODE_DESCRIPTION", StringType(), True),
				StructField("PRODUCT_DETAILS", StringType(), True),
				StructField("IMPORTER_TA_CODE", StringType(), False),
				StructField("EXPORTER_TA_CODE", StringType(), False),
				StructField("FOB_VALUE", FloatType(), True),
				StructField("FOB_CURRENCY", StringType(), True),
				StructField("FREIGHT_AMOUNT", FloatType(), True),
				StructField("INSURANCE_AMOUNT", FloatType(), True),
				StructField("F_I_CURRENCY", StringType(), True),
				StructField("CIF_VALUE", FloatType(), True),
				StructField("CIF_CURRENCY", StringType(), True),
				StructField("STATISTICAL_VALUE_USD", FloatType(), True),
				StructField("UNIT_PRICE", FloatType(), True),
				StructField("NET_WEIGHT", FloatType(), True),
				StructField("NET_WEIGHT_UNIT", StringType(), True),
				StructField("GROSS_WEIGHT", FloatType(), True),
				StructField("GROSS_WEIGHT_UNIT", StringType(), True),
				StructField("QUANTITY", FloatType(), True),
				StructField("QUANTITY_UNIT", StringType(), True),
				StructField("PACKAGE_AMOUNT", FloatType(), True),
				StructField("PACKAGES_UNIT", StringType(), True),
				StructField("INCOTERMS", StringType(), True),
				StructField("PAYMENT_TYPE", StringType(), True),
				StructField("TRANSPORT_TYPE", StringType(), True),
				StructField("REGIME", StringType(), True),
				StructField("BRAND_NAME", StringType(), True),
				StructField("MANUFACTURING_COMPANY", StringType(), True),
				StructField("COUNTRY_OF_ORIGIN", StringType(), False),
				StructField("PORT_OF_ARRIVAL", StringType(), True),
				StructField("PORT_OF_DEPARTURE", StringType(), True),
				StructField("CONDITION_New_Used", StringType(), True),
				StructField("NOTIFY_PARTY", StringType(), True),
				StructField("NOTIFY_ADDRESS", StringType(), True),
				StructField("TRANSPORT_COMPANY", StringType(), True),
				StructField("CONTAINER_COUNT", FloatType(), True),
				StructField("TOTAL_TEUS", FloatType(), True),
				StructField("EXPORT_DATA_TYPE", StringType(), False),
				StructField("IMPORT_DATA_TYPE", StringType(), False),
				StructField("ITEM_NO", FloatType(), True),
				StructField("DATA_TYPE", StringType(), False),
				StructField("VESSEL_NAME", StringType(), True),
				StructField("RECORD_ID", StringType(), False),
			]
		)

		return schema

	def convert_to_p7(self, df: DataFrame) -> DataFrame:
		# Tabloyu istenen formata dönüştür
		p7_schema = self.create_schema_p7()
		p7_df = self.spark.createDataFrame(df.rdd, p7_schema)

		# Orijinal kolon isimlerine uygun olmayan sütunları null olarak ayarla
		for column in df.columns:
			if column not in p7_df.columns:
				p7_df = p7_df.withColumn(
					column, col(column).cast("string").cast("string")
				)  # Var olan sütunlar string olarak dönüştürülecek

		return p7_df
