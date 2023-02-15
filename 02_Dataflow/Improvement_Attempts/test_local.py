import json
from apache_beam.transforms.core import Flatten
from apache_beam.transforms import Map

### EDEM MDA Data Project 2 Group 3
# Process Data with Dataflow

#Import beam libraries
import apache_beam as beam
from apache_beam.transforms import Map
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.combiners import MeanCombineFn
from apache_beam.transforms.core import CombineGlobally
import apache_beam.transforms.window as window
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io.gcp import bigquery_tools
from apache_beam.pvalue import AsDict
from apache_beam.transforms.core import Flatten
from apache_beam import PTransform
from apache_beam.transforms.core import FlatMap
from apache_beam.pvalue import AsIter

from functools import reduce

#Import common libraries
from datetime import datetime
import argparse
import json
import logging
import requests

## pcoll
pcoll1 = [("time", str(datetime.now()))]
pcoll2 = [('pressure_mean', 23), ('pressure_status', "red"), ("pressure_notification","Error! ")]
pcoll3 = [('temp_mean', 24), ('temp_status', "red"), ("temp_notification","Warning")]
pcoll4 = [('m_mean', 25), ('m_status', "red"), ("m_notification","OK! ")]


def inversePubSubMessage(element):
    output_json = json.dumps(element)
    #logging.info("encoding: %s", output_json)
    return output_json.encode('utf-8')

def convert_dict(element):
    return print(element)

def convert_dict1(element):
    new_dict = {element[0]:element[1]}
    return new_dict

joined_lists_12 = ((pcoll1, pcoll2)
    | "flatten 12" >> beam.Flatten() 
    )

joined_lists_34 = ((pcoll3, pcoll4)
    | "flatten 34" >> beam.Flatten() 
)

joined_lists = ((joined_lists_12, joined_lists_34)
    | "flatten all" >> beam.Flatten() 
        )
print(f' joined_lists: {joined_lists}')

dict_lists = (joined_lists
    | "convert" >> beam.CombineGlobally(lambda tuples: {k: v for k, v in tuples}).without_defaults())



print(f' dict_lists: {dict_lists}')
# Use the reduce function to combine all dictionaries into a single dictionary

# Combine the three PCollections into a single PCollection

endresult = (dict_lists
| "Encode" >> beam.Map(inversePubSubMessage)
)
print(endresult)


