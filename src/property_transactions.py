from operator import index
from tkinter.font import names
import apache_beam as beam
from apache_beam.io import ReadFromText
import datetime
import os
import pandas as pd
from apache_beam.dataframe.io import *
from apache_beam.dataframe.transforms import DataframeTransform
from apache_beam.dataframe.convert import to_pcollection

from collections import namedtuple

input_file_full = r'C:/projects/street_group_data_engineer/street_group_test/input_files/pp-complete.csv'
input_file_yearly = r'C:/Users/viraj/Downloads/pp-2021.csv'
output_prefix = os. getcwd() + r'/output_files/results_splitdata'



class Split(beam.DoFn):
    def process(self, element):
        element = element.split('","')
        element = element[0].split(',') + (element[1:])
        element = [e.replace('"','') for e in element]
        Transaction_id,price_paid,Transfer_date,post_code,property_type,old_or_new,duration,paon,saon, street,locality, Town,district,county,ppd_cat_type,record_status= element
        return [{'Transaction_id':Transaction_id,
        'price_paid':price_paid,
        'Transfer_date':Transfer_date,
        'post_code':post_code,
        'property_type':property_type,
        'old_or_new': old_or_new,
        'duration':duration,
        'PAON':paon,
        'SAON':saon, 
        'street':street,
        'locality':locality,
        'Town':Town,
        'district':district,
        'county':county,
        'ppd_cat_type':ppd_cat_type,
        'record_status':record_status
        }]

class unique_key_gen(beam.DoFn):
    def remove_space(k,v):
        return ''.join(k[v].split(' '))
    def convert(dictionary):
        return namedtuple('NameTuple', dictionary.keys())(**dictionary)

    def process(self, element):
        post_code = unique_key_gen.remove_space(element, 'post_code')
        paon = unique_key_gen.remove_space(element, 'PAON')
        saon = unique_key_gen.remove_space(element, 'SAON')
        locality = unique_key_gen.remove_space(element, 'locality')
        Town = unique_key_gen.remove_space(element, 'Town')
        district = unique_key_gen.remove_space(element, 'district')
        county = unique_key_gen.remove_space(element, 'county')
        unique_keygen_list = [post_code,paon,saon,locality,Town,district,county]
        unique_property_key = '_'.join(unique_keygen_list)
        key_dict = {'property_ID': unique_property_key,'transaction_details':element}

        my_tuple = (unique_property_key, element)
        results = []
        keyval= list((unique_property_key, element))
        results.append(keyval)
        #print(results)
        return results
        
        element = {**key_dict,**element}

        #key_dict = unique_key_gen.convert(key_dict)
        #element = beam.Row(element)
        print(my_tuple)

        return my_tuple

class get_tuple(beam.DoFn):
    
    def process(self, element):
        results = []
        keyval= list((element['date'], element['transaction_amount']))
        results.append(keyval)
        return results



p = beam.Pipeline()

inputdata  = p | 'read_data' >> ReadFromText(input_file_yearly, skip_header_lines=1)



split_data = inputdata | 'split' >> beam.ParDo(Split())
key_gen = split_data | 'key' >> beam.ParDo(unique_key_gen())
group = key_gen | 'grp' >> beam.GroupByKey()
jsonl_result = split_data |'jsonl_output' >> beam.io.WriteToText(file_path_prefix=output_prefix, file_name_suffix='.jsonl')

p.run()
