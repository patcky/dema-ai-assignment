from dataclasses import dataclass
import pandas as pd
import pandera as pa
import os
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
import logging, time
from uuid import uuid4

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
LOGGER = logging.getLogger(__name__)


import time

def main():
    """
    This is the main function that executes the data processing script.

    It performs the following steps:
    1. Retrieves environment variables and database connection details.
    2. Gets table schemas.
    3. Reads CSV files and processes the data.
    4. Validates the data against the table schema.
    5. Saves the processed data to the database.
    6. Saves any errors encountered during processing.
    7. Logs the script execution duration.

    """
    start = time.time()
    error_handler = errorHandler()
    env, username, password, database_name, database_schema, hostname, port = (
        get_env_variables()
    )
    table_schemas = get_table_schemas()

    time.sleep(3)
    engine = get_db_engine(database_name, username, password, hostname, port)
    error_handler.errors_table = Table(
        "errors", MetaData(), schema=database_schema, autoload_with=engine
    )

    for table_name, table_schema in table_schemas.items():
        if table_name == "products":
            df = read_csv("source-data/inventory.csv", error_handler)
        else:
            df = read_csv(f"source-data/{table_name}.csv", error_handler)

        if len(df) == 0:
            error_handler.add_error(uuid4(), table_name, "No data found in CSV file.")
            continue

        raw_table = Table(
            f"raw_{table_name}", MetaData(), schema=database_schema, autoload_with=engine
        )
        table = Table(
            table_name, MetaData(), schema=database_schema, autoload_with=engine
        )

        df.columns = [col.lower() for col in df.columns]

        validate_schema(df, table_schema, error_handler)

        df = df.where(pd.notnull(df), None)

        for row in df.itertuples(index=False):
            with engine.connect() as conn:
                save_to_raw_data_table(row, raw_table, conn, error_handler)

            with engine.connect() as conn:
                if validate_row_data(row, table, table_schema, error_handler):
                    upsert_row(row, table, conn, error_handler)

    with engine.connect() as conn:
        error_handler.save_errors(conn)

    duration = time.time() - start
    LOGGER.info(f"Script execution complete in {duration} seconds.")


def get_env_variables():
    """Get env variables from .env file

    Returns:
        str: Environment name
        str: Username
        str: Password
        str: Database name
        str: Database schema name
        str: Hostname
        str: Port
    """
    try:
        env = os.environ["ENV"]
        username = os.environ["POSTGRES_USER"]
        password = os.environ["POSTGRES_PASSWORD"]
        database_name = os.environ["POSTGRES_DB"]
        database_schema = os.environ["POSTGRES_SCHEMA"]
        hostname = os.environ["POSTGRES_HOST"]
        port = os.environ["POSTGRES_PORT"]
    except KeyError as e:
        LOGGER.error(f"Missing environment variable: {e}")
        raise e

    LOGGER.info(
        f"Executing script in ENV: {env}, DB: {database_name}, SCHEMA: {database_schema}, HOST: {hostname}, PORT: {port}"
    )
    return env, username, password, database_name, database_schema, hostname, port

def get_table_schemas():
    """Get table schemas for products and orders

    Returns:
        dict: Dictionary containing table schemas for products and orders
    """
    table_schemas = {}
    table_schemas["products"] = pa.DataFrameSchema(
        name="products",
        columns={
            "productid": pa.Column(str, nullable=False),
            "name": pa.Column(str, nullable=False),
            "quantity": pa.Column(pa.Int, nullable=False),
            "category": pa.Column(str, nullable=False),
            "subcategory": pa.Column(str, nullable=False),
        },
        unique=["productid"],
    )
    table_schemas["orders"] = pa.DataFrameSchema(
        name="orders",
        columns={
            "orderid": pa.Column(str, nullable=False),
            "productid": pa.Column(str, nullable=False),
            "currency": pa.Column(str, nullable=False),
            "quantity": pa.Column(int, nullable=False),
            "shippingcost": pa.Column(float, nullable=False),
            "amount": pa.Column(float, nullable=False),
            "channel": pa.Column(str, nullable=False),
            "channelgroup": pa.Column(str, nullable=False),
            "campaign": pa.Column(str, nullable=True),
            "datetime": pa.Column(
                str,
                nullable=False,
                coerce=True,
                checks=pa.Check.str_matches(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z"),
            ),
        },
        unique=["orderid"],
    )
    return table_schemas

def read_csv(file_path, error_handler):
    """Read CSV file and return DataFrame

    Args:
        file_path (str): Path to CSV file
        error_handler (errorHandler): Error handler object

    Returns:
        pd.DataFrame: DataFrame containing data from CSV file
    """
    LOGGER.info(f"Reading CSV file: {file_path}")
    try:
        return pd.read_csv(file_path, index_col=False)
    except Exception as e:
        error_handler.add_error(
            uuid4(), file_path, f"Error reading CSV file: {file_path}. Error: {e}"
        )
        return pd.DataFrame()


def get_db_engine(db_name, user, password, hostname, port):
    """Create database engine for PostgreSQL

    Args:
        db_name (str): Database name
        user (str): Username
        password (str): Password
        hostname (str): Hostname
        port (str): Port

    Returns:
        Engine: Database engine object
    """
    LOGGER.info(
        f"Creating database engine for {db_name}, host: {hostname}, port: {port}"
    )
    return create_engine(
        f"postgresql://{user}:{password}@{hostname}:{port}/{db_name}"
    )


def validate_schema(df, schema, error_handler):
    """Validate schema of DataFrame against schema object using pandera

    Args:
        df (pd.DataFrame): DataFrame to validate
        schema (panderas.DataFrameSchema): Schema to validate against
        error_handler (errorHandler): Error handler object
    """
    LOGGER.info(f"Validating schema for {schema.name}")
    try:
        schema.validate(df, lazy=True, inplace=True)

    except pa.errors.SchemaErrors as e:
        error_handler.add_error(
            uuid4(), schema.name, f"Error validating schema for {schema.name}: {e}"
        )


def save_to_raw_data_table(row, table, conn, error_handler):
    """Save data to raw table in database

    Args:
        row (pd.Tuple): Row data
        table (Table): Table object
        conn (Connection): Database connection object
        error_handler (errorHandler): Error handler object
    '"""
    try:
        conn.begin()
        payload = row._asdict()
        timestamp = str(datetime.now())
        result = conn.execute(
            insert(table).values(payload=payload, timestamp=timestamp)
        )
        conn.commit()
        if not result:
            raise Exception("No rows inserted into raw table.")
    except Exception as e:
        error_handler.add_error(
            uuid4(),
            table.name,
            f"Error saving data to raw table raw_{table.name}. Error: {e}",
        )


def validate_row_data(row, table, table_schema, error_handler):
    """Validate row data and send errors to error handler.

    Args:
        row (pd.Tuple): Row data
        table (Table): Table object
        table_schema (pandera.DataFrameSchema): Table schema object
        error_handler (errorHandler): Error handler object

    Returns:
        bool: True if row data is valid, False otherwise
    """
    optional_columns = set(
        col_name
        for col_name, col_object in table_schema.columns.items()
        if col_object.nullable
    )
    error_fields = []
    for col_name, col_value in row._asdict().items():
        col_value = getattr(row, col_name)
        schema_column_type = table_schema.dtypes[col_name].type

        # Check if column is null but is optional or if column type is correct
        if (pd.isnull(col_value) and col_name in optional_columns) or schema_column_type.__eq__(type(col_value)):
            continue
        error_fields.append((col_name, col_value))

    if error_fields:
        error_handler.add_error(
            uuid4(),
            table.name,
            f"Invalid fields for columns in row {row[0]}: {error_fields}",
        )
        return False
    return True


def upsert_row(row, table, conn, error_handler):
    """Upsert row data into table

    Args:
        row (pd.Series): Row data
        table (Table): Table object
        conn (Connection): Database connection object
        error_handler (errorHandler): Error handler object
    """
    try:
        primary_key = table.primary_key.columns.values()[0].name
        row_data = row._asdict()
        update_fields = row_data.copy()
        del update_fields[primary_key]
        stmt = (
            insert(table)
            .values(row_data)
            .on_conflict_do_update(index_elements=[primary_key], set_=update_fields)
        )
        conn.begin()
        result = conn.execute(stmt)
        if result is None or result.rowcount == 0:
            raise Exception("Could not insert row.")
        conn.commit()
    except Exception as e:
        error_handler.add_error(uuid4(), table.name, f"Error inserting row {row[0]}: {e}.")


class errorHandler:
    def __init__(self):
        self.errors = []
        self.errors_table = None

    @dataclass
    class errorRecord:
        recordid: str
        recordtype: str
        errors: str
        timestamp: datetime = str(datetime.now())

    def _log_error(self, recordid, recordtype, error):
        LOGGER.error(f"Record: {recordtype} - id: {recordid} - error: {error}")

    def add_error(self, recordid, recordtype, error):
        self._log_error(recordid, recordtype, error)
        self.errors.append(self.errorRecord(recordid, recordtype, error))

    def save_errors(self, conn):
        LOGGER.info("Saving errors to database")
        if self.errors:
            conn.begin()
            for e in self.errors:
                conn.execute(
                    insert(self.errors_table).values(e.__dict__)
                )
            conn.commit()
            conn.close()

if __name__ == "__main__":
    main()
