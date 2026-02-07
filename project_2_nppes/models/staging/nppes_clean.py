import polars as pl

def model(dbt, session):
    nppes = pl.from_pandas(dbt.ref("nppes_raw").df())
    
    nppes_clean = nppes.with_columns(
        pl.col(["Provider Organization Name (Legal Business Name)", 
     "Provider Last Name (Legal Name)", "Provider First Name", "Provider Middle Name", "Provider Business Practice Location Address City Name"]).str.to_titlecase())
    
    nppes_clean = nppes_clean.unique(["Provider Organization Name (Legal Business Name)", 
     "Provider Last Name (Legal Name)", "Provider First Name", "Provider Middle Name"])

    nppes_clean = nppes_clean.with_columns(
    pl.concat_str(
        [
            pl.col("Provider Last Name (Legal Name)"),
            pl.lit(", "),
            pl.col("Provider First Name"),
            pl.when(pl.col("Provider Middle Name").is_not_null())
              .then(pl.concat_str([pl.lit(" "), pl.col("Provider Middle Name")]))
              .otherwise(pl.lit(""))
        ]
    ).alias("provider_name")
)
    nppes_clean = nppes_clean.with_columns(
    pl.when(
        pl.col("provider_name").str.replace_all(r"[, ]+", "").eq("")
    )
    .then(pl.col("Provider Organization Name (Legal Business Name)"))
    .otherwise(pl.col("provider_name"))
    .alias("provider_name")
).unique(['provider_name'])

    nppes_clean = nppes_clean.with_columns(
        pl.col("Provider Business Practice Location Address Postal Code").str.slice(0, 5).alias("ZIP"))

    nppes_clean = nppes_clean.rename({"Healthcare Provider Taxonomy Code_1": "Taxonomy_Code", 
                                     "Provider Business Practice Location Address State Name": "state_abbv"}).rename(lambda name: name.replace(" ", "_"))

    return nppes_clean