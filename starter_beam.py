# Beam Aggregation Flow

# This code heavily follows the Beam tutorials for wordcount:
# https://beam.apache.org/get-started/wordcount-example/
# I'm attempting to make notes as possible to help with comprehension!

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText
import json
import argparse
import re

from past.builtins import unicode

# Setup Configuration for Pipeline Object
# These can be passed at runtime, should also associate this with a config file
# to pass additional parameters that may be hardcoded.
class RuntimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input', help='Pipeline Input', default='gs://dataflow-samples/shakespeare/kinglear.txt')
        parser.add_argument('--output', help='Output file for pipeline')
        parser.add_argument('--runner', help='Setup Runner', default='DirectRunner')

# Define a Do Function to pull ParDo transform out of the main loop
# Do functions are based on the `process` function definition
class FormatTextDo(beam.DoFn):
    def process(self, element):
        elm1, elm2 = element
        yield '%s: %s' % (elm1, elm2)

# Define a Composite transform to stick together multiple DoFunctions
# Composite transforms are based on the `expand` function definition
class CountingWordsPT(beam.PTransform):
    def expand(self, pcoll):
        return (
            # Define transform piped objects
            pcoll
            # Pull text lines into word elements
            | 'ExtractWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
            # Count word occurrances with built in counter...
            | beam.combiners.Count.PerElement()
        )

def counting_pipeline():
    # Pull Data from gs public bucket into beam
    options = RuntimeOptions()
    with beam.Pipeline(options=options) as p:
        # Reading File into PCollection
        lines = p | beam.io.ReadFromText(options.input)
        # Count Word Occurrances
        # Inline example below replaced with Composite Transform 
        counts = lines | CountingWordsPT()
        ''' counts = (
            lines
            | 'Split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x)).with_output_types(unicode))
            | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
        )
        '''
        # Format the counts into PCollection of Strings
        # Inline example below replaced with defined Do function
        output = counts | beam.ParDo(FormatTextDo())
        '''
        output = counts | 'Format' >> beam.Map(format_result)
        '''
        # Write result to "Write" transform
        output | WriteToText(options.output)

if __name__ == '__main__':
    counting_pipeline()
