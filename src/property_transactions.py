import apache_beam as beam
from apache_beam.io import ReadFromText
import datetime
import os

input_file_full = r'C:/projects/street_group_data_engineer/street_group_test/input_files/pp-complete.csv'
input_file_monthly = r'C:/projects/street_group_data_engineer/street_group_test/input_files/pp-monthly-update-new-version.csv'
output_prefix = os. getcwd() + r'/output_files/results'


p = beam.Pipeline()

input = p | 'read_data' >> ReadFromText(input_file_monthly, skip_header_lines=1)
#op = input | 'show_data' >> beam.Map(print)
jsonl_result = input |'jsonl_output' >> beam.io.WriteToText(file_path_prefix=output_prefix, file_name_suffix='.jsonl.gz',
header=['Transaction_id','price_paid', 'Transfer_date', 'post_code','property_type','old/new','duration','PAON','SAON', 'street','locality', 'Town','district','county','ppd_cat_type','record_status'
])

p.run()
