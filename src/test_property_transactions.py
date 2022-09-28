import unittest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import apache_beam as beam
from property_transactions import tranctions_composite_transform


class TestTransactions(unittest.TestCase):

  def test_transactions(self):

    input_data = ['''"{D707E536-3C15-0AD9-E053-6B04A8C067CC}","415000","2021-07-16 00:00","B69 1QQ","D","N","F","150","","OAKHAM ROAD","TIVIDALE","OLDBURY","SANDWELL","WEST MIDLANDS","A","A"''',
'''"{D707E536-3C16-0AD9-E053-6B04A8C067CC}","240000","2021-07-30 00:00","WR2 5NQ","S","N","F","33","","BLENHEIM ROAD","","WORCESTER","WORCESTER","WORCESTERSHIRE","A","A"''',
'''"{D707E536-3C17-0AD9-E053-6B04A8C067CC}","213000","2021-08-26 00:00","B97 6EH","S","N","F","237","","BIRMINGHAM ROAD","","REDDITCH","REDDITCH","WORCESTERSHIRE","A","A"''',
'''"{D707E536-3C18-0AD9-E053-6B04A8C067CC}","472000","2021-09-13 00:00","WR3 7JD","T","N","F","53","","PENBURY STREET","","WORCESTER","WORCESTER","WORCESTERSHIRE","A","A"''',
'''"{CFC9085D-5A23-9A70-E053-6B04A8C09D6A}","130000","2021-08-03 00:00","M34 6EF","T","N","F","40","","LINDEN ROAD","DENTON","MANCHESTER","TAMESIDE","GREATER MANCHESTER","B","A"''',
'''"{CFC9085D-5A24-9A70-E053-6B04A8C09D6A}","146000","2021-10-07 00:00","M34 6EF","T","N","F","40","","LINDEN ROAD","DENTON","MANCHESTER","TAMESIDE","GREATER MANCHESTER","B","A"''',
'''"{D707E536-5C1A-0AD9-E053-6B04A8C067CC}","223000","2021-12-20 00:00","LS13 4AE","T","N","F","1","","SOMERDALE MEWS","BRAMLEY","LEEDS","LEEDS","WEST YORKSHIRE","A","A"''',
'''"{D707E536-5C1B-0AD9-E053-6B04A8C067CC}","223000","2021-12-20 00:00","LS13 4AE","T","N","F","1","","SOMERDALE MEWS","BRAMLEY","LEEDS","LEEDS","WEST YORKSHIRE","A","A"'''
]

    expected_op = [{'Property_ID': 'B691QQ_150__TIVIDALE_OLDBURY_SANDWELL_WESTMIDLANDS', 'Transaction_Details': [{'Transaction_id': '{D707E536-3C15-0AD9-E053-6B04A8C067CC}', 'price_paid': '415000', 'Transfer_date': '2021-07-16 00:00', 'post_code': 'B69 1QQ', 'property_type': 'D', 'old_or_new': 'N', 'duration': 'F', 'PAON': '150', 'SAON': '', 'street': 'OAKHAM ROAD', 'locality': 'TIVIDALE', 'Town': 'OLDBURY', 'district': 'SANDWELL', 'county': 'WEST MIDLANDS', 'ppd_cat_type': 'A', 'record_status': 'A'}]},
{'Property_ID': 'WR25NQ_33___WORCESTER_WORCESTER_WORCESTERSHIRE', 'Transaction_Details': [{'Transaction_id': '{D707E536-3C16-0AD9-E053-6B04A8C067CC}', 'price_paid': '240000', 'Transfer_date': '2021-07-30 00:00', 'post_code': 'WR2 5NQ', 'property_type': 'S', 'old_or_new': 'N', 'duration': 'F', 'PAON': '33', 'SAON': '', 'street': 'BLENHEIM ROAD', 'locality': '', 'Town': 'WORCESTER', 'district': 'WORCESTER', 'county': 'WORCESTERSHIRE', 'ppd_cat_type': 'A', 'record_status': 'A'}]},
{'Property_ID': 'B976EH_237___REDDITCH_REDDITCH_WORCESTERSHIRE', 'Transaction_Details': [{'Transaction_id': '{D707E536-3C17-0AD9-E053-6B04A8C067CC}', 'price_paid': '213000', 'Transfer_date': '2021-08-26 00:00', 'post_code': 'B97 6EH', 'property_type': 'S', 'old_or_new': 'N', 'duration': 'F', 'PAON': '237', 'SAON': '', 'street': 'BIRMINGHAM ROAD', 'locality': '', 'Town': 'REDDITCH', 'district': 'REDDITCH', 'county': 'WORCESTERSHIRE', 'ppd_cat_type': 'A', 'record_status': 'A'}]},
{'Property_ID': 'WR37JD_53___WORCESTER_WORCESTER_WORCESTERSHIRE', 'Transaction_Details': [{'Transaction_id': '{D707E536-3C18-0AD9-E053-6B04A8C067CC}', 'price_paid': '472000', 'Transfer_date': '2021-09-13 00:00', 'post_code': 'WR3 7JD', 'property_type': 'T', 'old_or_new': 'N', 'duration': 'F', 'PAON': '53', 'SAON': '', 'street': 'PENBURY STREET', 'locality': '', 'Town': 'WORCESTER', 'district': 'WORCESTER', 'county': 'WORCESTERSHIRE', 'ppd_cat_type': 'A', 'record_status': 'A'}]},
{'Property_ID': 'M346EF_40__DENTON_MANCHESTER_TAMESIDE_GREATERMANCHESTER', 'Transaction_Details': [{'Transaction_id': '{CFC9085D-5A23-9A70-E053-6B04A8C09D6A}', 'price_paid': '130000', 'Transfer_date': '2021-08-03 00:00', 'post_code': 'M34 6EF', 'property_type': 'T', 'old_or_new': 'N', 'duration': 'F', 'PAON': '40', 'SAON': '', 'street': 'LINDEN ROAD', 'locality': 'DENTON', 'Town': 'MANCHESTER', 'district': 'TAMESIDE', 'county': 'GREATER MANCHESTER', 'ppd_cat_type': 'B', 'record_status': 'A'}, {'Transaction_id': '{CFC9085D-5A24-9A70-E053-6B04A8C09D6A}', 'price_paid': '146000', 'Transfer_date': '2021-10-07 00:00', 'post_code': 'M34 6EF', 'property_type': 'T', 'old_or_new': 'N', 'duration': 'F', 'PAON': '40', 'SAON': '', 'street': 'LINDEN ROAD', 'locality': 'DENTON', 'Town': 'MANCHESTER', 'district': 'TAMESIDE', 'county': 'GREATER MANCHESTER', 'ppd_cat_type': 'B', 'record_status': 'A'}]},
{'Property_ID': 'LS134AE_1__BRAMLEY_LEEDS_LEEDS_WESTYORKSHIRE', 'Transaction_Details': [{'Transaction_id': '{D707E536-5C1A-0AD9-E053-6B04A8C067CC}', 'price_paid': '223000', 'Transfer_date': '2021-12-20 00:00', 'post_code': 'LS13 4AE', 'property_type': 'T', 'old_or_new': 'N', 'duration': 'F', 'PAON': '1', 'SAON': '', 'street': 'SOMERDALE MEWS', 'locality': 'BRAMLEY', 'Town': 'LEEDS', 'district': 'LEEDS', 'county': 'WEST YORKSHIRE', 'ppd_cat_type': 'A', 'record_status': 'A'}, {'Transaction_id': '{D707E536-5C1B-0AD9-E053-6B04A8C067CC}', 'price_paid': '223000', 'Transfer_date': '2021-12-20 00:00', 'post_code': 'LS13 4AE', 'property_type': 'T', 'old_or_new': 'N', 'duration': 'F', 'PAON': '1', 'SAON': '', 'street': 'SOMERDALE MEWS', 'locality': 'BRAMLEY', 'Town': 'LEEDS', 'district': 'LEEDS', 'county': 'WEST YORKSHIRE', 'ppd_cat_type': 'A', 'record_status': 'A'}]}]


    with TestPipeline() as p:
        input = p | beam.Create(input_data)
        output = input | tranctions_composite_transform()
        assert_that(output, equal_to(expected_op), label='CheckOutput')


if __name__ == '__main__':
    unittest.main()