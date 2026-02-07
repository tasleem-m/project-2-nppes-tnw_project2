import polars as pl

def model(dbt, session):
    population = pl.from_pandas(dbt.ref("population_raw").df())
    states = pl.from_pandas(dbt.ref("states_clean").df())
    
    population_clean = population.with_columns(
            pl.col("NAME")
            .str.splitn(",", 2)
            .struct.rename_fields(["county_name", "state_name"])
        ).unnest("NAME")
    
    population_clean = population_clean.with_columns(
        pl.col("state_name").str.strip_chars())
    
    population_clean = population_clean.with_columns(
        pl.col(["POP", "state", "county"]).cast(pl.Int64))
    
    population_clean = population_clean.with_columns(
        pl.concat_str(["state", "county"]).str.zfill(5).alias("county_code"))
    
    population_clean = population_clean.join(states, on="state_name", how="left").unique(["county_name", "state_name", "county_code"])
    population_clean = population_clean.rename({"state_right": "state_abbv"})
    
    return population_clean
