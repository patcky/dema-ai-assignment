import unittest
from sqlalchemy import create_engine
from main import create_schema

class TestCreateSchema(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        self.table_schemas = {
            'products': {
                'columns': {
                    'productId': 'String',
                    'name': 'String',
                    'quantity': 'Integer',
                    'category': 'String',
                    'subCategory': 'String'
                },
                'index': ['category', 'subCategory']
            },
            'orders': {
                'columns': {
                    'orderId': 'String',
                    'productId': 'String',
                    'currency': 'String',
                    'quantity': 'Integer',
                    'shippingCost': 'Float',
                    'amount': 'Float',
                    'channel': 'String',
                    'channelGroup': 'String',
                    'campaign': 'String',
                    'dateTime': 'DateTime'
                },
                'index': ['channel', 'channelGroup', 'campaign', 'dateTime'],
                'optional': ['campaign'],
                'relationships': {
                    'productId': 'products.productId'
                }
            }
        }

    def test_create_schema(self):
        create_schema(self.table_schemas, self.engine)
        metadata = self.engine.metadata
        products_table = metadata.tables['products']
        orders_table = metadata.tables['orders']

        # Check column types
        self.assertEqual(str(products_table.c.productId.type), 'VARCHAR')
        self.assertEqual(str(products_table.c.name.type), 'VARCHAR')
        self.assertEqual(str(products_table.c.quantity.type), 'INTEGER')
        self.assertEqual(str(products_table.c.category.type), 'VARCHAR')
        self.assertEqual(str(products_table.c.subCategory.type), 'VARCHAR')

        self.assertEqual(str(orders_table.c.orderId.type), 'VARCHAR')
        self.assertEqual(str(orders_table.c.productId.type), 'VARCHAR')
        self.assertEqual(str(orders_table.c.currency.type), 'VARCHAR')
        self.assertEqual(str(orders_table.c.quantity.type), 'INTEGER')
        self.assertEqual(str(orders_table.c.shippingCost.type), 'FLOAT')
        self.assertEqual(str(orders_table.c.amount.type), 'FLOAT')
        self.assertEqual(str(orders_table.c.channel.type), 'VARCHAR')
        self.assertEqual(str(orders_table.c.channelGroup.type), 'VARCHAR')
        self.assertEqual(str(orders_table.c.campaign.type), 'VARCHAR')
        self.assertEqual(str(orders_table.c.dateTime.type), 'DATETIME')

        # Check indexes
        self.assertEqual(len(products_table.indexes), 1)
        self.assertEqual(len(orders_table.indexes), 1)
        self.assertEqual(str(products_table.indexes[0].columns), '[products.category, products.subCategory]')
        self.assertEqual(str(orders_table.indexes[0].columns), '[orders.channel, orders.channelGroup, orders.campaign, orders.dateTime]')

        # Check optional columns
        self.assertTrue(orders_table.c.campaign.nullable)

        # Check foreign key relationship
        self.assertEqual(str(orders_table.c.productId.foreign_keys), '{ForeignKey(\'products.productId\')}')


if __name__ == '__main__':
    unittest.main()
import unittest
from unittest.mock import patch
from io import StringIO
from main import main

class TestMain(unittest.TestCase):
    @patch('sys.stdout', new_callable=StringIO)
    def test_main(self, mock_stdout):
        main()
        output = mock_stdout.getvalue()
        self.assertIn("Script execution complete", output)

if __name__ == '__main__':
    unittest.main()
