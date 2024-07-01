from config.DBConfig import PostgresConfig, S3Config


class CostaRicaTypes:
    IMPORT_CHANGE_COLUMN_NAMES = {
        "DATE": "ARRIVAL_DATE",
        "OPERATION#": "DECLARATION_NUMBER",
        "ITEM": "ITEM_NO",
        "IMPORTER#": "IMPORTER_TAX_ID",
        "IMPORTER": "IMPORTER_NAME",
        "HS CODE": "HS_CODE",
        "GOODS DESCRIPTION": "PRODUCT_DETAILS",
        "COUNTRY_ORIGIN": "COUNTRY_OF_ORIGIN",
        "COUNTRY_ACQUISITION": "yedek_EXPORTER_COUNTRY",
        "SHIPPING_COUNTRY": "EXPORTER_COUNTRY",
        "QUANTITY": "QUANTITY",
        "UNIT": "QUANTITY_UNIT",
        "VALUE_(USD)": "CIF_VALUE",
        "NET_WEIGHT_(KG)": "NET_WEIGHT",
        "GROSS_WEIGHT_(KG)": "GROSS_WEIGHT",
        "INGRESS_CUSTOMS": "PORT_OF_ARRIVAL",
    }

    IMPORT_ADD_COLUMN = {
        "NET_WEIGHT_UNIT": ("STRING", "Kilogram"),
        "GROSS_WEIGHT_UNIT": ("STRING", "Kilogram"),
        "CIF_CURRENCY": ("STRING", "USD"),
        "IMPORTER_COUNTRY": ("STRING", "CR"),
        "PORT_OF_DEPARTURE": ("STRING", ""),
        "IMPORTER_TA_CODE": ("STRING", ""),
        "EXPORTER_TA_CODE": ("STRING", ""),
        "DATA_TYPE": ("STRING", "Custom"),
        "EXPORT_DATA_TYPE": ("INTEGER", "4"),
        "IMPORT_DATA_TYPE": ("INTEGER", "1"),
        "RECORD_ID": ("UUID", "''"),
    }

    EXPORT_CHANGE_COLUMN_NAMES = {
        "DATE": "ARRIVAL_DATE",
        "OPERATION#": "DECLARATION_NUMBER",
        "ITEM": "ITEM_NO",
        "IMPORTER#": "IMPORTER_TAX_ID",
        "IMPORTER": "IMPORTER_NAME",
        "HS CODE": "HS_CODE",
        "GOODS DESCRIPTION": "PRODUCT_DETAILS",
        "COUNTRY ORIGIN": "COUNTRY_OF_ORIGIN",
        "COUNTRY ACQUISITION": "yedek_EXPORTER_COUNTRY",
        "SHIPPING COUNTRY": "EXPORTER_COUNTRY",
        "QUANTITY": "QUANTITY",
        "UNIT": "QUANTITY_UNIT",
        "VALUE (USD)": "CIF_VALUE",
        "NET WEIGHT (KG)": "NET_WEIGHT",
        "GROSS WEIGHT (KG)": "GROSS_WEIGHT",
        "INGRESS CUSTOMS": "PORT_OF_ARRIVAL",
    }

    EXPORT_ADD_COLUMN = {
        "ARRIVAL_DATE": ("DATE", "1900-01-01"),  # Doğru tarih formatı
        "NET_WEIGHT_UNIT": ("STRING", "Kilogram"),
        "GROSS_WEIGHT_UNIT": ("STRING", "Kilogram"),
        "CIF_CURRENCY": ("STRING", "USD"),
        "IMPORTER_COUNTRY": ("STRING", "CR"),
        "PORT_OF_DEPARTURE": ("STRING", ""),
        "IMPORTER_TA_CODE": ("STRING", ""),
        "EXPORTER_TA_CODE": ("STRING", ""),
        "DATA_TYPE": ("STRING", "Custom"),
        "EXPORT_DATA_TYPE": ("INTEGER", "4"),
        "IMPORT_DATA_TYPE": ("INTEGER", "1"),
        "RECORD_ID": ("UUID", ""),
    }
    HOST = PostgresConfig.HOST
    PORT = PostgresConfig.PORT
    USER = PostgresConfig.USER
    PASSWORD = PostgresConfig.PASSWORD
    DATABASE = "new_data"
    SCHEMA = "CR"
    country_code = "CR"
