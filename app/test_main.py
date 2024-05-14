import unittest
from unittest.mock import patch, mock_open, MagicMock
from uuid import uuid4
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
from main import (
    read_csv,
    get_env_variables,
    get_table_schemas,
    get_db_engine,
    validate_schema,
    save_to_raw_data_table,
    validate_row_data,
    upsert_row,
    errorHandler,
)


class TestMain(unittest.TestCase):
    def setUp(self):
        self.csv_data = "productid,name,quantity,category,subcategory\n1,Product 1,10,Category A,Subcategory 1\n2,Product 2,20,Category B,Subcategory 2\n"
        self.csv_data_empty = ""
        self.env = {
            "ENV": "test",
            "POSTGRES_USER": "test_user",
            "POSTGRES_PASSWORD": "test_password",
            "POSTGRES_DB": "test_db",
            "POSTGRES_SCHEMA": "public",
            "POSTGRES_HOST": "localhost",
            "POSTGRES_PORT": "5432",
        }

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data="productid,name,quantity,category,subcategory\n1,Product 1,10,Category A,Subcategory 1\n2,Product 2,20,Category B,Subcategory 2\n",
    )
    def test_read_csv(self, mock_open):
        # Test reading CSV file
        df = read_csv("test.csv", errorHandler())
        mock_open.assert_called_with(
            "test.csv", "r", encoding="utf-8", errors="strict", newline=""
        )
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)

    @patch.dict(
        "os.environ",
        {
            "ENV": "test",
            "POSTGRES_USER": "test_user",
            "POSTGRES_PASSWORD": "test_password",
            "POSTGRES_DB": "test_db",
            "POSTGRES_SCHEMA": "public",
            "POSTGRES_HOST": "localhost",
            "POSTGRES_PORT": "5432",
        },
    )
    def test_get_env_variables(self):
        # Test getting environment variables
        expected_env = (
            "test",
            "test_user",
            "test_password",
            "test_db",
            "public",
            "localhost",
            "5432",
        )
        result = get_env_variables()
        self.assertEqual(result, expected_env)

    def test_get_table_schemas(self):
        # Test getting table schemas
        schemas = get_table_schemas()
        self.assertIn("products", schemas)
        self.assertIn("orders", schemas)
        self.assertEqual(len(schemas["products"].columns), 5)
        self.assertEqual(len(schemas["orders"].columns), 10)

    @patch("sqlalchemy.create_engine")
    def test_get_db_engine(self, mock_create_engine):
        # Test getting database engine
        mock_engine = MagicMock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        engine = get_db_engine(*list(self.env.values())[-5:])
        self.assertIsInstance(engine, Engine)

    def test_validate_schema(self):
        # Test schema validation
        schema = get_table_schemas()["products"]
        df_valid = pd.DataFrame(
            {
                "productid": ["abc123", "def456"],
                "name": ["Product 1", "Product 2"],
                "quantity": [10, 20],
                "category": ["Category A", "Category B"],
                "subcategory": ["Subcategory 1", "Subcategory 2"],
            }
        )
        df_invalid = pd.DataFrame(
            {
                "productid": [1234, 1235],
                "name": ["Product 1", "Product 2"],
                "quantity": ["foo", 20],
                "category": ["Category A", "Category B"],
                "subcategory": ["Subcategory 1", "Subcategory 2"],
            }
        )
        error_handler = errorHandler()
        validate_schema(df_valid, schema, error_handler)
        self.assertFalse(error_handler.errors)
        validate_schema(df_invalid, schema, error_handler)
        self.assertTrue(error_handler.errors)

    def test_save_to_raw_data_table(self):
        # Test saving data to raw data table
        metadata = MetaData()
        table = Table(
            "raw_products",
            metadata,
            Column("productid", String),
            Column("name", String),
            Column("quantity", Integer),
            Column("category", String),
            Column("subcategory", String),
        )
        row = (
            {
                "productid": "abc1236",
                "name": "Product 1",
                "quantity": 10,
                "category": "Category A",
                "subcategory": "Subcategory 1",
            }
        )
        error_handler = errorHandler()
        save_to_raw_data_table(row, table, MagicMock(spec=Engine), error_handler)
        # Add assertions here

    def test_validate_row_data(self):
        # Test row data validation
        schema = get_table_schemas()["products"]
        table = Table(
            "products",
            MetaData(),
            Column("productid", String),
            Column("name", String),
            Column("quantity", Integer),
            Column("category", String),
            Column("subcategory", String),
        )
        # Create a DataFrame with a single row
        data = {
            "productid": ["abc123"],
            "name": ["Product 1"],
            "quantity": [10],
            "category": ["Category A"],
            "subcategory": ["Subcategory 1"],
        }
        df = pd.DataFrame(data)
        # Convert the DataFrame to a DataFrame.itertuples object
        rows = df.itertuples(index=False)
        # Validate the row data
        error_handler = errorHandler()
        for row in rows:
            self.assertTrue(validate_row_data(row, table, schema, error_handler))

    def test_upsert_row(self):
        # Test upserting row into table
        engine = MagicMock(spec=Engine)
        metadata = MetaData()
        table = Table(
            "products",
            metadata,
            Column("productid", String, primary_key=True),
            Column("name", String),
            Column("quantity", Integer),
            Column("category", String),
            Column("subcategory", String),
        )
        # Create a DataFrame with a single row
        data = {
            "productid": ["abc123"],
            "name": ["Product 1"],
            "quantity": [10],
            "category": ["Category A"],
            "subcategory": ["Subcategory 1"],
        }
        df = pd.DataFrame(data)
        # Convert the DataFrame to a DataFrame.itertuples object
        rows = df.itertuples(index=False)
        # Upsert the row into the table
        error_handler = errorHandler()
        for row in rows:
            try:
                upsert_row(row, table, engine, error_handler)
            except KeyError as e:
                error_handler.add_error(uuid4(), table.name, f"Error inserting row {row.productid}: {e}.")
        # Assert the result
        with engine.connect() as conn:
            result = conn.execute(table.select()).fetchall()
            #self.assertEqual(len(result), 1)


if __name__ == "__main__":
    unittest.main()
