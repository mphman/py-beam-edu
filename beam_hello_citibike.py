# CitiBike Test Beam aggregation flow

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText
import argparse

# Setup Pipeline Configuration
class RuntimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input', help='Pipeline Input')
        parser.add_argument('--output', help='Output file for pipeline')
        parser.add_argument('--runner', help='Setup Runner', default='DirectRunner')

# Define ParDo to Select Columns of Interest and populate Collection
class DataShapeDo(beam.DoFn):
    def process(self, element):
        from datetime import datetime
        (tripduration,
        starttime,
        stoptime,
        start_station_id,
        start_station_name,
        start_station_latitude,
        start_station_longitude,
        end_station_id,
        end_station_name,
        end_station_latitude,
        end_station_longitude,
        bikeid,
        usertype,
        birth_year,
        gender,
        customer_plan) = element.split(",")
        return [{
            'timestamp' : datetime.fromisoformat(starttime),
            'start_id' : int(start_station_id)
        }]

# Write ParDo to aggregate by Station ID and Month(Date)
class MonthStationKeyDo(beam.DoFn):
    def process(self, element):
        from datetime import datetime
        return [(element['start_id'], element['timestamp'].month)]

class FormatResultDo(beam.DoFn):
    def process(self, element):
        elm1, elm2, elm3 = element
        yield '%s - %d: %d' % (elm1,elm2,elm3)

# Write Pipeline
def hello_citibike_pipeline():
    """
    Example Pipeline to Count Citibike Trips month over month based on station ID
    """
    options = RuntimeOptions()
    with beam.Pipeline(options=options) as p:
        data_import_tripdata = (p
                                | "Reading Dataset to Beam" >> beam.io.ReadFromText(options.input, skip_header_lines=1)
                                | "Splitting and Selecting Columns" >> beam.ParDo(DataShapeDo())
        )
        data_group_aggregation = (data_import_tripdata
                                  | "Define Month & Station Key" >> beam.ParDo(MonthStationKeyDo())
                                  | "Define Keys" >> beam.GroupByKey()
                                  | "Define 1 for Count" >> beam.Map(lambda x: (x[0],1))
                                  | "Generate Counts" >> beam.CombinePerKey(sum)
        )
        # output = data_group_aggregation | beam.ParDo(FormatResultDo())
        data_group_aggregation | WriteToText(options.output)

if __name__ == '__main__':
    hello_citibike_pipeline()