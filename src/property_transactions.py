import apache_beam as beam
from apache_beam.io import ReadFromText
import os
from collections import namedtuple
from pathlib import Path

#create input directory and input file
Path(os. getcwd() + r'/input_files/').mkdir(parents=True, exist_ok=True)
input_file_yearly = os.getcwd() + r'/input_files/pp-2021.csv'

#create output directory if it doesn't exist.
Path(os. getcwd() + r'/output_files/').mkdir(parents=True, exist_ok=True)
output_prefix = os. getcwd() + r'/output_files/results'

#Check input
file_exists = os.path.exists(input_file_yearly)

if not file_exists:
    print('Input file not found. Code exiting. Please place input data file in <current working directory>/input_files/<file_name>.csv')
    exit(1)


#split input data and return with field names
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



#Generate unqiue key using property data
class unique_key_gen(beam.DoFn):
    def remove_space(k,v):
        return ''.join(k[v].split(' '))

    def process(self, element):
        results = []
        post_code = unique_key_gen.remove_space(element, 'post_code')
        paon = unique_key_gen.remove_space(element, 'PAON')
        saon = unique_key_gen.remove_space(element, 'SAON')
        locality = unique_key_gen.remove_space(element, 'locality')
        Town = unique_key_gen.remove_space(element, 'Town')
        district = unique_key_gen.remove_space(element, 'district')
        county = unique_key_gen.remove_space(element, 'county')
        unique_keygen_list = [post_code,paon,saon,locality,Town,district,county]
        unique_property_key = '_'.join(unique_keygen_list)
        keyval= list((unique_property_key, element))
        results.append(keyval)
        return results

#Unpack tuple data to get data in proper format. {Property_ID:<property_details>,Transaction_Details:[<transactions>]}
class unpack_tuple(beam.DoFn):
    def process(self, element):
        element_dict = {'Property_ID':element[0], 'Transaction_Details':element[1]}
        return [element_dict]



class tranctions_composite_transform(beam.PTransform):

  def expand(self, pcollect):
    return (
        pcollect 
    
    | 'split_data' >> beam.ParDo(Split())
    | 'keygen' >> beam.ParDo(unique_key_gen())
    | 'group_property_id' >> beam.GroupByKey()
    | 'unpack_tuple' >> beam.ParDo(unpack_tuple())
    )



if __name__ == '__main__':
    p = beam.Pipeline()
    input_data = p | 'read_data' >> ReadFromText(input_file_yearly, skip_header_lines=1)
    result = input_data | tranctions_composite_transform()
    output_data = result  | 'jsonl_output' >> beam.io.WriteToText(file_path_prefix=output_prefix, file_name_suffix='.jsonl')
    p.run()
    print('Pipeline Run Completed!!')