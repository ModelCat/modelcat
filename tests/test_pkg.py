import unittest
import modelcat.connector as connector
import logging as log


class TestPkg(unittest.TestCase):
    def test_version_attr(self):
        self.assertTrue(hasattr(connector, "__version__"))
        self.assertIsInstance(connector.__version__, str)


if __name__ == '__main__':
    log.getLogger().setLevel(log.INFO)
    unittest.main()
