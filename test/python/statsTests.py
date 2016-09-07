import unittest
from statsTransformer import *

class MetricsTestCase(unittest.TestCase):
    def testED(self):
        data = '{"read":"2015-01-08T22:57:31.547920715Z","network":{"rx_dropped":0,"rx_bytes":648,"rx_errors":0,"tx_packets":8,"tx_dropped":0,"rx_packets":8,"tx_errors":0,"tx_bytes":648},"memory_stats":{"stats":{"total_pgmajfault":0,"cache":0,"mapped_file":0,"total_inactive_file":0,"pgpgout":414,"rss":6537216,"total_mapped_file":0,"writeback":0,"unevictable":0,"pgpgin":477,"total_unevictable":0,"pgmajfault":0,"total_rss":6537216,"total_rss_huge":6291456,"total_writeback":0,"total_inactive_anon":0,"rss_huge":6291456,"hierarchical_memory_limit":67108864,"total_pgfault":964,"total_active_file":0,"active_anon":6537216,"total_active_anon":6537216,"total_pgpgout":414,"total_cache":0,"inactive_anon":0,"active_file":0,"pgfault":964,"inactive_file":0,"total_pgpgin":477},"max_usage":6651904,"usage":6537216,"failcnt":0,"limit":67108864},"blkio_stats":{},"cpu_stats":{"cpu_usage":{"percpu_usage":[16970827,1839451,7107380,10571290],"usage_in_usermode":10000000,"total_usage":36488948,"usage_in_kernelmode":20000000},"system_cpu_usage":20091722000000000,"throttling_data":{}},"precpu_stats":{"cpu_usage":{"percpu_usage":[16970827,1839451,7107380,10571290],"usage_in_usermode":10000000,"total_usage":36488948,"usage_in_kernelmode":20000000},"system_cpu_usage":20091722000000000,"throttling_data":{}}}'
        result = createEDDict(data, "foo_0", "foo", "container", "container", "host", 4)
        self.assertTrue(result["trial_id"] == "foo_0")
        self.assertTrue(result["experiment_id"] == "foo")
        self.assertTrue(result["container_id"] == "container")
        self.assertTrue(result["container_name"] == "container")
        self.assertTrue(result["cpu_total_usage"] == 36488948)
        self.assertTrue(result["cpu_percent_usage"] == 0)
        self.assertTrue(result["cpu_percpu_usage"] == [16970827,1839451,7107380,10571290])
        self.assertTrue(result["cpu_percpu_percent_usage"] == [0,0,0,0])
        self.assertTrue(result["cpu_cores"] == 4)
        self.assertTrue(result["memory_usage"] == 6537216/(1024*1024))
        self.assertTrue(result["read_time"] == "2015-01-08T22:57:31.547920715Z")
    def testIO(self):
        data = '{"read":"2015-11-25T13:13:50.067865758Z","network":{},"memory_stats":{"stats":{"cache":48545792,"mapped_file":16297984,"total_inactive_file":29720576,"pgpgout":5436,"rss":189509632,"total_mapped_file":16297984,"pgpgin":39027,"pgmajfault":187,"total_rss":189509632,"total_rss_huge":100663296,"total_inactive_anon":4096,"rss_huge":100663296,"hierarchical_memory_limit":9223372036854771712,"total_pgfault":28593,"total_active_file":18800640,"active_anon":189530112,"total_active_anon":189530112,"total_pgpgout":5436,"total_cache":48545792,"inactive_anon":4096,"active_file":18800640,"pgfault":28593,"inactive_file":29720576,"total_pgpgin":39027},"max_usage":240197632,"usage":238055424,"limit":2099945472},"blkio_stats":{"io_service_bytes_recursive":[{"major":8,"op":"Read","value":35676160},{"major":8,"op":"Write","value":12673024},{"major":8,"op":"Sync","value":12668928},{"major":8,"op":"Async","value":35680256},{"major":8,"op":"Total","value":48349184}],"io_serviced_recursive":[{"major":8,"op":"Read","value":894},{"major":8,"op":"Write","value":31},{"major":8,"op":"Sync","value":30},{"major":8,"op":"Async","value":895},{"major":8,"op":"Total","value":925}]},"cpu_stats":{"cpu_usage":{"percpu_usage":[377944515],"usage_in_usermode":80000000,"total_usage":377944515,"usage_in_kernelmode":250000000},"system_cpu_usage":131160000000,"throttling_data":{}},"precpu_stats":{"cpu_usage":{},"throttling_data":{}}}'
        result = createIODict(data, "foo_0", "foo", "container", "host")
        self.assertTrue(result[0]["writes"] == 12673024)
        self.assertTrue(result[0]["reads"] == 35676160)
        self.assertTrue(result[0]["total"] == 48349184)
        self.assertTrue(result[0]["async"] == 35680256)
        self.assertTrue(result[0]["sync"] == 12668928)

if __name__ == '__main__':
    unittest.main()