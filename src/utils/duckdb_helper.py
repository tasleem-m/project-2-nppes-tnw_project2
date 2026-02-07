import duckdb

def convert_to_duckdb(schema, table_name, data, logger):
    conn = duckdb.connect('project_2_nppes/dev.duckdb')

    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    conn.execute(f"CREATE OR REPLACE TABLE {schema}.{table_name} AS SELECT * FROM data")

    logger.info(f"DuckDB table {schema}.{table_name} created and/or updated")

    print(conn.execute(f"SHOW TABLES FROM {schema}").fetchall())

    conn.close()