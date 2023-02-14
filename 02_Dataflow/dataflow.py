### EDEM MDA Data Project 2 Group 3
# Process Data with Dataflow

#Import beam libraries
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.combiners import MeanCombineFn, ToList
from apache_beam.transforms.core import CombineGlobally
import apache_beam.transforms.window as window
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io.gcp import bigquery_tools

#Import common libraries
from datetime import datetime
import argparse
import json
import logging
import requests

""" Helpful functions """
#Decode PubSub message & convert it in json-format in order to deal with
def ParsePubSubMessage(message):
    pubsubmessage = message.data.decode('utf-8')
    row = json.loads(pubsubmessage)
    logging.info("Receiving message from PubSub:%s", pubsubmessage)
    return row

""" DoFn Classes """
#DoFn: Extract time, temperature, pressure, motor power from data
class agg_temperature(beam.DoFn):
    def process(self, element):
        temperature = element['temperature']
        yield temperature

class agg_motorpower(beam.DoFn):
    def process(self, element):
        motorpower = element['motor_power']
        yield motorpower

class agg_pressure(beam.DoFn):
    def process(self, element):
        pressure = element['pressure']
        yield pressure

#DoFn: Add Window ProcessingTime & Status depending if measured value is within the optimum range
class status_temp(beam.DoFn):
    def process(self, element):
        if element >= 45 and element <= 47:
            output_data = {'processingTime': str(datetime.now()), 'mean': element, 'status': "temp_green", "notification":"The temperature is in the optimum range."}
        elif element >= 44 and element <45 or element > 47 and element <=48:
            output_data = {'processingTime': str(datetime.now()), 'mean': element, 'status': "temp_yellow", "notification":"Caution! Actions might be neccessary, as the measured temperature is out of the optimum range."}
        else:
            output_data = {'processingTime': str(datetime.now()), 'mean': element, 'status': "temp_red", "notification":"Error! Machine is not working properly, the temperature is way out of the optimum range. Help is needed."}
        output_json = json.dumps(output_data)
        logging.info("Adding status & its notification: %s", output_json)
        yield output_json.encode('utf-8')

#DoFn: Add Window ProcessingTime & Status depending if measured value is within the optimum range
class status_pressure(beam.DoFn):
    def process(self, element):
        if element >= 60 and element <= 70:
            output_data = {'processingTime': str(datetime.now()), 'mean': element, 'status': "pressure_green", "notification":"The pressure is in the optimum range."}
        elif element >= 58 and element < 60 or element > 70 and element <= 72:
            output_data = {'processingTime': str(datetime.now()), 'mean': element, 'status': "pressure_yellow", "notification":"Caution! Actions might be neccessary, as the measured pressure is out of the optimum range."}
        else:
            output_data = {'processingTime': str(datetime.now()), 'mean': element, 'status': "pressure_red", "notification":"Error! Machine is not working properly, the pressure is way out of the optimum range. Help is needed."}
        output_json = json.dumps(output_data)
        logging.info("Adding status & its notification: %s", output_json)
        yield output_json.encode('utf-8')

#DoFn: Add Window ProcessingTime & Status depending if measured value is within the optimum range
class status_mpower(beam.DoFn):
    def process(self, element):
        if element >= 11 and element <= 13:
            output_data = {'processingTime': str(datetime.now()), 'mean': element, 'status': "mpower_green", "notification":"The motor power is in the optimum range."}
        elif element >= 9 and element < 11  or element > 13 and element <= 15:
            output_data = {'processingTime': str(datetime.now()), 'mean': element, 'status': "mpower_yellow", "notification":"Caution! Actions might be neccessary, as the measured motor power is out of the optimum range."}
        else:
            output_data = {'processingTime': str(datetime.now()), 'mean': element, 'status': "mpower_red", "notification":"Error! Machine is not working properly, the motor power is way out of the optimum range. Help is needed."}
        output_json = json.dumps(output_data)
        logging.info("Adding status & its notification: %s", output_json)
        yield output_json.encode('utf-8')

""" Dataflow Process """
def run():
    #Define input arguments
    parser = argparse.ArgumentParser(description=('Arguments for the Dataflow Streaming Pipeline.'))
    parser.add_argument(
                    '--project_id',
                    required=True,
                    help='GCP cloud project name')
    parser.add_argument(
                    '--input_subscription',
                    required=True,
                    help='PubSub Subscription which will be the source of data.')
    parser.add_argument(
                    '--output_topic',
                    required=True,
                    help='PubSub Topic which will be the sink for notification data.')
    parser.add_argument(
                    '--output_bigquery',
                    required=True,
                    help='Table where data will be stored in BigQuery. Format: <dataset>.<table>.')
    parser.add_argument(
                    '--bigquery_schema_path',
                    required=False,
                    default='./schemas/bq_schema.json',
                    help='BigQuery Schema Path within the repository.')              
    
    args, pipeline_opts = parser.parse_known_args()

    """ BigQuery Table Schema """
    #Load schema from /schema folder
    with open(args.bigquery_schema_path) as file:
        input_schema = json.load(file)

    schema = bigquery_tools.parse_table_schema_from_json(json.dumps(input_schema))

    """ Apache Beam Pipeline """
    #Pipeline Options
    options = PipelineOptions(pipeline_opts, save_main_session=True, streaming=True, project=args.project_id)

    #Create the pipeline: 
    with beam.Pipeline(argv=pipeline_opts,options=options) as p:
        #Part01: Read messages from PubSub & parse JSON messages with Map Function
        data = (
            p | "Read messages from PubSub" >>  beam.io.ReadFromPubSub(subscription=f"projects/{args.project_id}/subscriptions/{args.input_subscription}", with_attributes=True)
              | "Parse JSON messages" >> beam.Map(ParsePubSubMessage)
        )

        #Part02: Write proccessing message to Big Query
        (data | "Write to BigQuery" >>  beam.io.WriteToBigQuery(
            table = f"{args.project_id}:{args.output_bigquery}", 
            schema = schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        ))

        #Part03: Calculate the mean of temperature per minute and put that data with its related status into PubSub
        (data 
            | "Temp: Get value" >> beam.ParDo(agg_temperature())
            | "Temp: WindowByMinute" >> beam.WindowInto(window.FixedWindows(60))
            | "Temp: MeanByWindow" >> beam.CombineGlobally(MeanCombineFn()).without_defaults()
            | "Temp: Add Status & WindowProcessingTime" >>  beam.ParDo(status_temp())
            | "Temp: WriteToPubSub" >>  beam.io.WriteToPubSub(topic=f"projects/{args.project_id}/topics/{args.output_topic}", with_attributes=False)
        )

        #Part04: Calculate the mean of pressure per minute and put that data with its related status into PubSub
        (data 
            | "Pressure: Get value" >> beam.ParDo(agg_pressure())
            | "Pressure: WindowByMinute" >> beam.WindowInto(window.FixedWindows(60))
            | "Pressure: MeanByWindow" >> beam.CombineGlobally(MeanCombineFn()).without_defaults()
            | "Pressure: Add Status & WindowProcessingTime" >>  beam.ParDo(status_pressure())
            | "Pressure: WriteToPubSub" >>  beam.io.WriteToPubSub(topic=f"projects/{args.project_id}/topics/{args.output_topic}", with_attributes=False)
        )
        
        #Part05: Calculate the mean of motor power per minute and put that data with its related status into PubSub
        (data 
            | "MPower: Get motor power value" >> beam.ParDo(agg_motorpower())
            | "MPower: WindowByMinute" >> beam.WindowInto(window.FixedWindows(60))
            | "MPower: MeanByWindow" >> beam.CombineGlobally(MeanCombineFn()).without_defaults()
            | "MPower: Add Add Status & WindowProcessingTime" >>  beam.ParDo(status_mpower())
            | "MPower: WriteToPubSub" >>  beam.io.WriteToPubSub(topic=f"projects/{args.project_id}/topics/{args.output_topic}", with_attributes=False)
        )
#Run generator process (for writing data to BigQuery & to second/output PubSub Topic inkl. logging)
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
