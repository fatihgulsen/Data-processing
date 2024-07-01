from typing import Literal

from config.DataTypes.CR import CostaRicaTypes
from Spark import Spark4DataProc


class CostaRica(Spark4DataProc):

    def __init__(self, table_name, appName="CostaRicaDataProcessing"):

        super().__init__(
            f"{CostaRicaTypes.HOST}:{CostaRicaTypes.PORT}",
            CostaRicaTypes.USER,
            CostaRicaTypes.PASSWORD,
            appName,
        )
        self.database = CostaRicaTypes.DATABASE
        if table_name:
            self.table = table_name
        else:
            raise (ValueError("Contains Table Name"))
        self.country_code = CostaRicaTypes.country_code

    def determine_data_type(self, table_name) -> Literal['IMPORT'] | Literal['EXPORT']:
        """Tablo adına göre veri tipini belirler."""
        if "IMP" in table_name.upper():
            return "IMPORT"
        elif "EXP" in table_name.upper():
            return "EXPORT"
        else:
            raise ValueError(f"Unknown data type for table: {table_name}")

    def get_column_details(self, data_type):# -> dict[str, Any]:
        """İthalat ve İhracat veri tiplerine göre sütun değişikliklerini belirler."""
        if data_type == "IMPORT":
            return {
                "rename": CostaRicaTypes.IMPORT_CHANGE_COLUMN_NAMES,
                "add": CostaRicaTypes.IMPORT_ADD_COLUMN,
            }
        elif data_type == "EXPORT":
            return {
                "rename": CostaRicaTypes.EXPORT_CHANGE_COLUMN_NAMES,
                "add": CostaRicaTypes.EXPORT_ADD_COLUMN,
            }
        else:
            raise ValueError("Data Type should be either 'IMPORT' or 'EXPORT'")

    def finalize_dataframe(self, df):
        """DataFrame üzerinde son düzenlemeleri yapar."""
        df = self.add_columns_by_type(df)
        return self.reorder_and_maintain_columns(df)

    def update_columns(self):
        """Güncelleme işlemlerini yönetir, sütun isimlerini ve yeni sütunları ekler."""
        cr_df = self.read_table(self.country_code + "." + self.table, self.database)
        data_type = self.determine_data_type(self.table)
        column_details = self.get_column_details(data_type)

        cr_df = self.rename_columns(cr_df, column_details["rename"])
        cr_df = self.add_columns(cr_df, column_details["add"])
        cr_df = self.finalize_dataframe(cr_df)

        self.write_table(
            cr_df, self.country_code + "." + self.table+'_islendi', self.database, mode="overwrite"
        )

    def update_ta_codes(self):
        cr_df = self.read_table(
            table_name=self.country_code + "." + self.table+'_islendi', database_name=self.database
        )
        self.update_exporter_ta_code(cr_df)
        self.update_importer_ta_code(cr_df)
        self.write_table(
            df=cr_df,
            table_name=self.country_code + "." + self.table+'_islendi',
            database_name=self.database,
            mode="overwrite",
        )

    def update_country(self):
        cr_df = self.read_table(
            table_name=self.country_code + "." + self.table+'_islendi', database_name=self.database
        )
        cr_df = self.update_country_of_origin(cr_df)
        cr_df = self.update_exporter_country(cr_df)
        cr_df = self.update_importer_country(cr_df)
        cr_df = self.update_port_of_arrival(cr_df)
        cr_df = self.update_port_of_departure(cr_df)
        self.write_table(
            df=cr_df,
            table_name=self.country_code + "." + self.table+'_islendi',
            database_name=self.database,
            mode="overwrite",
        )

    def update_hs_code(self):
        cr_df = self.read_table(
            table_name=self.country_code + "." + self.table+'_islendi', database_name=self.database
        )
        cr_df = self.update_hs_code_description(cr_df, "CR")
        self.write_table(
            df=cr_df,
            table_name=self.country_code + "." + self.table+'_islendi',
            database_name=self.database,
            mode="overwrite",
        )
        pass

    def update_quantity(self):
        cr_df = self.read_table(
            table_name=self.country_code + "." + self.table+'_islendi', database_name=self.database
        )
        cr_df = self.update_quantity_and_unit(cr_df, "CR")
        self.write_table(
            df=cr_df,
            table_name=self.country_code + "." + self.table+'_islendi',
            database_name=self.database,
            mode="overwrite",
        )

    def comprehensive_checks(self):
        cr_df = self.read_table(
            table_name=self.country_code + "." + self.table, database_name=self.database
        )
        """Veri bütünlüğünü kapsamlı bir şekilde kontrol eder."""
        checks = {
            "null_checks": self.check_null_columns(
                cr_df,
                [
                    "PORT_OF_ARRIVAL",
                    "PORT_OF_DEPARTURE",
                    "HS_CODE",
                    "HS_CODE_DESCRIPTION",
                    "ARRIVAL_DATE",
                    "IMPORTER_TA_CODE",
                    "EXPORTER_TA_CODE",
                    "QUANTITY",
                    "QUANTITY_UNIT",
                    "COUNTRY_OF_ORIGIN",
                ],
            ),
            "invalid_country_codes": self.check_missing_country(
                cr_df, "COUNTRY_OF_ORIGIN"
            ).collect(),
            "hs_code_integrity": self.check_hs_code_integrity(cr_df),
            "ta_code_issues": self.check_ta_codes(
                cr_df, ["IMPORTER_TA_CODE", "EXPORTER_TA_CODE"]
            ),
            "quantity_integrity": self.check_quantity_integrity(
                cr_df, self.country_code
            ),
        }
        return checks
