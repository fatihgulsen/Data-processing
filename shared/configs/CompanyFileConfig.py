class CompanyFileType:
    fields = {
        "TA_CODE": {"column_no": 0, "validation_rules": ["exist", "ta_code"]},
        "COMPANY_TAX_ID": {"column_no": 1, "validation_rules": ["text"]},
        "COMPANY_NAME": {"column_no": 2, "validation_rules": ["exist", "text"]},
        "COMPANY_ADDRESS": {"column_no": 3, "validation_rules": ["text"]},
        "CITY_STATE": {"column_no": 4, "validation_rules": ["text"]},
    }
