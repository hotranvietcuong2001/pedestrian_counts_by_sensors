import logging
from airflow.hooks.postgres_hook import PostgresHook

def non_empty_check(postgres_conn_id, *args, **kwargs):
    # get list of tables
    tables = kwargs["params"]["tables"]

    # form connection to redshift
    redshift = PostgresHook(postgres_conn_id=postgres_conn_id)

    # run query and check results for each table
    for table in tables:
        results = redshift.get_records(f"SELECT COUNT(*) FROM {table}")

        if results[0][0] < 1 or results[0][0] == "0":
            raise ValueError(f"Data quality check failed. {table} is empty!")

        logging.info(f"Data quality check passed. {table} has {results[0][0]} records.")

def unique_values_check(postgres_conn_id, *args, **kwargs):
    # get tables and columns
    tables_and_columns = kwargs["params"]["tables_and_columns"]

    # form connection to redshift
    redshift = PostgresHook(postgres_conn_id=postgres_conn_id)

    # run checks on each table and column combination
    for table, column in tables_and_columns.items():
        total_count = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
        distinct_count = redshift.get_records(f"SELECT COUNT(DISTINCT {column}) FROM {table}")

        if total_count[0][0] != distinct_count[0][0]:
            raise ValueError(
                f"Check failed for {table}. Total count is {total_count[0][0]}. Distinct count is {distinct_count[0][0]}."
            )
        
        logging.info(
            f"Data quality check passed. Total and distinct counts are the same for the {column} column in {table}"
        )
