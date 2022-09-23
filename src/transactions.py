import apache_beam as beam
from apache_beam.io import ReadFromText
import datetime
import os


input_file = r'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'
output_prefix = os. getcwd() + r'/output/results.csv'


class Split(beam.DoFn):
    def process(self, element):
        timestamp,origin,destination,transaction_amount = element.split(',')
        return [{
            'date': timestamp,
            'transaction_amount': float(transaction_amount),
        }]

class filter_amt(beam.DoFn):
    
    def process(self, element):
        results = []
        if element['transaction_amount'] > 20:
            results.append(element)
        return results

class filter_year(beam.DoFn):
    
    def process(self, element):
        results = []
        bool_val = (datetime.datetime.strptime(element['date'] , '%Y-%m-%d %H:%M:%S %Z').year > 2010)
        if bool_val:
            results.append(element)
        return results

class get_tuple(beam.DoFn):
    
    def process(self, element):
        results = []
        keyval= list((element['date'], element['transaction_amount']))
        results.append(keyval)
        return results
      
class join_list(beam.DoFn):

    def process(self, element):
        temp = []
        join_list = list(element)
        temp.append(join_list)
        #print(amount)
        #print(','.join(join_list))
        
        return temp

class write_list(beam.DoFn):
    def setup(self):
        exist = os.path.isfile(output_prefix)
        if exist:
            os.remove(output_prefix)
    def process(self,element):
        result_list = [str(element[0]),str(element[1])]
        result_string = ','.join(result_list)
        exist = os.path.isfile(output_prefix)
        if exist:
            mode = 'a'
        else:
            with open(output_prefix, 'w') as f:
                f.write('date, Total_amount' + '\n')
                mode = 'a'
        with open(output_prefix, mode) as f:
            f.write(result_string + '\n')



class tranctions_composite_transform(beam.PTransform):

  def expand(self, pcollect):
    return (
        pcollect 
    | 'read_data' >> ReadFromText(input_file, skip_header_lines=1)
    | 'split_data' >> beam.ParDo(Split())
    | 'filter_amount' >> beam.ParDo(filter_amt())
    | 'filter_years' >> beam.ParDo(filter_year())
    | 'convert_data' >> beam.ParDo(get_tuple())
    | 'sum_amount' >> beam.CombinePerKey(sum)
    | 'final_output' >> beam.ParDo(join_list())
    )


if __name__ == '__main__':
    p = beam.Pipeline()
    result = p | tranctions_composite_transform()
    csv_result = result | 'csv_output' >> beam.ParDo(write_list())
    jsonl_result = result |'jsonl_output' >> beam.io.WriteToText(file_path_prefix=output_prefix, file_name_suffix='.jsonl.gz', header=['date', 'Total_amount'])
    p.run()
    print('Pipeline Run Completed!!')