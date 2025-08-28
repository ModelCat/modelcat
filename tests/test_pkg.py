import unittest
import modelcat.modelcatconnector as modelcatconnector
import logging as log


class TestPkg(unittest.TestCase):
    def test_version_attr(self):
        self.assertTrue(hasattr(modelcatconnector, "__version__"))
        self.assertIsInstance(modelcatconnector.__version__, str)


if __name__ == '__main__':
    log.getLogger().setLevel(log.INFO)
    unittest.main()
