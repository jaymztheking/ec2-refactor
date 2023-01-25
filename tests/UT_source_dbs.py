import unittest

from ingestion.db import Postgres
from ingestion.config import pgdict

class PostgresAsSource(unittest.TestCase):
    def setUp(self) -> None:
        self.pgobject = Postgres(**pgdict)

    def test_connect(self):
        self.assertTrue(self.pgobject.connect())

    def test_disconnect(self):
        self.pgobject.connect()
        self.assertTrue(self.pgobject.disconnect())

    def test_row_count(self):
        self.pgobject.connect()
        count = self.pgobject.get_row_count('testschema', 'testtable')
        self.pgobject.disconnect()
        self.assertEqual(count, 3)

    def test_standalone_row_count(self):
        count = self.pgobject.get_row_count('testschema', 'testtable')
        self.assertEqual(count, 3)

    def tearDown(self) -> None:
        pass

if __name__ == '__main__':
    unittest.main()