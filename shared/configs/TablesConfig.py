class PostgresTablesConfig:
    DATA_PROC_DB = "ta_data_proc"
    UNIT_OF_QUANTITY_SCHEMA = "unit_of_quantity"
    COUNTRY_CODE_SCHEMA = "country_port_code"
    HS_CODE_DESC_SCHEMA = "hs_desc"
    TA_CODE_SCHEMA = "yeni_veri_kod_atama_uniq"

    RAW_DATA_DB = "ta_data_proc"
    RAW_DATA_FORMAT = (
        "yeni_veri_[a-z]_20[0-9]{2}_[0-9]{2}"  # Example : yeni_veri_cr_2023_12
    )
