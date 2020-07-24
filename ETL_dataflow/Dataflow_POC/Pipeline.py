from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window
from google.cloud import pubsub_v1
from google.cloud import bigquery
import apache_beam as beam
import logging
import argparse
import sys
import re

PROJECT = "My-GCE-project1"  # The name of our project
TOPIC = "projects/my-gce-project1/topics/my-gce-project1"  # Pub/sub topic
schema = 'timelocal:STRING, name:STRING, email:STRING, birthdate:STRING, service:STRING, plan:STRING, amount:STRING, status:String'
schema2 = 'service:String, amount:Integer'




###using regex to work with the strings , we want to collect our future rows from the string:

def regex_clean(data):  # AWESOME
    PATTERNS = [r'\d+/\w+/\d*:\d*:\d*:\d*', r'(?<=-\s)(\D*)(?=\sEmail)',
                r'\w*\.?\w*@\w*[/|.|-]?\w*[.\w+]*', r'(?<=\s)(\d*\/\d*\/\d*)(?=\s)',
                r'(?<=\s)(theft|auto|surgery|dental|clinic|reinsurance|others)(?=\s)',
                r'(?<=\s)(gold|diamond|silver|black|white)(?=\s\d)',
                r'(?<=\s)([1][\d]{2,3}|[2-9][\d]{2,3}|10000)(?=\s(true|false))', r'(?<=\d\s)(true|false)']
    result = []
    for match in PATTERNS:
        try:
            reg_match = re.search(match, data).group()
            if reg_match:
                result.append(reg_match)
            else:
                result.append(" ")
        except:
            print("There was an error with the regex search")
    result = [x.strip() for x in result]
    result = [x.replace('"', "") for x in result]
    res = ','.join(result)
    return res


# Split return as dictionary the strings collected on the regex function
class Split(beam.DoFn):

    def process(self, element):
        from datetime import datetime
        element = element.split(",")
        d = datetime.strptime(element[0], "%d/%b/%Y:%H:%M:%S")
        d2 = datetime.strptime(element[3], "%d/%m/%Y")  # birthdate is different from local date
        date_string = d.strftime("%d-%m-%Y %H:%M:%S")
        date_string2 = d2.strftime("%d-%m-%Y")
        return [{
            'timelocal': date_string,
            'name': element[1],
            'email': element[2],
            'birthdate': date_string2,
            'service': element[4],
            'plan': element[5],
            'amount': element[6],
            'status': element[7]
        }]




def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_topic")
    parser.add_argument("--output")
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions())
    
    (p
        |'ReadData' >> beam.io.ReadFromPubSub(topic=TOPIC).with_output_types(bytes)
        |"Decode" >> beam.Map(lambda x: x.decode('utf-8'))
        |"Clean Data" >> beam.Map(regex_clean)
        |'ParseCSV' >> beam.ParDo(Split())
        | 'WriteToBigQuery1' >> beam.io.WriteToBigQuery('my-gce-project1:china.POC_BEAM', schema=schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )


    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    main()