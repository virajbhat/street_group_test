import unittest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import apache_beam as beam
from transactions import tranctions_composite_transform


class TestTransactions(unittest.TestCase):

  def test_transactions(self):

    input_data = ['2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99',
                 '2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95',
                 '2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22',
                 '2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,1.00030',
                 '2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,13700000023.08',
                 '2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12',
                 ]

    expected_op = [['2017-03-18 14:09:16 UTC', 2102.22],
                  ['2017-08-31 17:00:09 UTC', 13700000023.08],
                  ['2018-02-27 16:04:11 UTC', 129.12]
                  ]


    with TestPipeline() as p:
        input = p | beam.Create(input_data)
        output = input | tranctions_composite_transform()

        assert_that(output, equal_to(expected_op), label='CheckOutput')


if __name__ == '__main__':
    unittest.main()