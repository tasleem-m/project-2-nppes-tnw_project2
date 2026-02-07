import polars as pl

def model(dbt, session):
    zip_county = pl.from_pandas(dbt.ref("zip_county_raw").df())

    zip_county_clean = zip_county.with_columns(
        pl.col("USPS_ZIP_PREF_CITY").str.to_titlecase())
    
    zip_county_clean = zip_county_clean.rename({"COUNTY": "county_code"})
    
    return zip_county_clean