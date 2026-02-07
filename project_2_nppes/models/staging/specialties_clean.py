import polars as pl

def model(dbt, session):
    provider_specialties = pl.from_pandas(dbt.ref("provider_specialties_raw").df())
    
    provider_specialties_clean = provider_specialties.rename({"Code": "Taxonomy_Code"})

    return provider_specialties_clean