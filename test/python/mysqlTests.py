import unittest
from mysqlTransformer import * 

class MetricsTestCase(unittest.TestCase):
    def test(self):
        conf = {'dest_table': 'process', 'src_table': 'ACT_HI_PROCINST', 'column_mapping': {'END_TIME_': 'end_time', 'START_TIME_': 'start_time', 'PROC_INST_ID_': 'source_process_instance_id', 'DURATION_': 'duration', 'PROC_DEF_ID_': 'process_definition_id'}}
        types = {u'ID_': u'"varchar(64)"', u'PROC_DEF_KEY_': u'"varchar(255)"', u'BUSINESS_KEY_': u'"varchar(255)"', u'END_ACT_ID_': u'"varchar(255)"', u'DURATION_': u'"bigint(20)"', u'END_TIME_': u'"datetime"', u'SUPER_CASE_INSTANCE_ID_': u'"varchar(64)"', u'PROC_INST_ID_': u'"varchar(64)"', u'DELETE_REASON_': u'"varchar(4000)"', u'START_TIME_': u'"datetime"', u'START_USER_ID_': u'"varchar(255)"', u'CASE_INST_ID_': u'"varchar(64)"', u'START_ACT_ID_': u'"varchar(255)"', u'SUPER_PROCESS_INSTANCE_ID_': u'"varchar(64)"', u'PROC_DEF_ID_': u'"varchar(64)"'}
        schema = [u'"ID_"', u'"PROC_INST_ID_"', u'"BUSINESS_KEY_"', u'"PROC_DEF_KEY_"', u'"PROC_DEF_ID_"', u'"START_TIME_"', u'"END_TIME_"', u'"DURATION_"', u'"START_USER_ID_"', u'"START_ACT_ID_"', u'"END_ACT_ID_"', u'"SUPER_PROCESS_INSTANCE_ID_"', u'"SUPER_CASE_INSTANCE_ID_"', u'"CASE_INST_ID_"', u'"DELETE_REASON_"\n']
        indexes = {u'END_TIME_': 6, u'START_TIME_': 5, u'PROC_INST_ID_': 1, u'DURATION_': 7, u'PROC_DEF_ID_': 4}
        data = [u'"00280ba2-ed02-11e5-beb9-7a98732174cb"', u'"00280ba2-ed02-11e5-beb9-7a98732174cb"', u'"NULL"', u'"additional-approval"', u'"additional-approval:1:76a1b53a-ed00-11e5-beb9-7a98732174cb"', u'"2016-03-18 12:07:35"', u'"2016-03-18 12:07:35"', u'"1"', u'"NULL"', u'"StartEvent_1"', u'"EndEvent_1"', u'"NULL"', u'"NULL"', u'"NULL"', u'"NULL"\n']
        result = createDic(data, "foo_0", "foo", conf, types, schema, indexes)
        self.assertTrue(result["start_time"] == '2016-03-18 12:07:35')
        self.assertTrue(result["end_time"] == '2016-03-18 12:07:35')
        self.assertTrue(result["duration"] == "1")
        self.assertTrue(result["process_definition_id"] == "additional-approval:1:76a1b53a-ed00-11e5-beb9-7a98732174cb")
        self.assertTrue(result["source_process_instance_id"] == "00280ba2-ed02-11e5-beb9-7a98732174cb")
        print(result)

if __name__ == '__main__':
    unittest.main()