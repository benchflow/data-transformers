import unittest
from propertiesTransformer import *
from testData import *

class MetricsTestCase(unittest.TestCase):
    def test(self):
        result = createContainerDict(MetricsTestCaseData, "foo_0", "foo", "container", "default")
        print result
        self.assertTrue(result["name"] == "db")
        self.assertTrue(result["memory_swappiness"] == -1)
        self.assertTrue(result["image"] == 'mysql')
        self.assertTrue("MYSQL_ROOT_PASSWORD=test" in result["environment"] \
                        and "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" in result["environment"] \
                        and "MYSQL_MAJOR=5.7" in result["environment"] \
                        and "MYSQL_VERSION=5.7.9-1debian8" in result["environment"])
        self.assertTrue(result["host_id"] == "default")
        self.assertTrue(result["restart_policy"] == "no")
        self.assertTrue(result["log_driver"] == "json-file")

if __name__ == '__main__':
    unittest.main()