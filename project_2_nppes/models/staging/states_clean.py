import polars as pl

def model(dbt, session):
    states = pl.from_pandas(dbt.ref("states_raw").df())
    
    states_clean = states.with_columns(pl.col("state_name").str.to_titlecase()).unique().drop_nulls()
    
    return states_clean