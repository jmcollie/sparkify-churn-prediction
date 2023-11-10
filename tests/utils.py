import unittest
from pyspark.sql import SparkSession
from dataclasses import dataclass, asdict, InitVar, field


class PySparkTestCase(unittest.TestCase):
    """A class for defining setUp and tearDown methods
    for testing class.
    """
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession \
                    .builder \
                   .appName("Testing Transformations") \
                   .getOrCreate()
    @classmethod
    def tearDownClass(cls):
        if cls.spark.getActiveSession():
            cls.spark.stop()

    
@dataclass 
class AssertFramesEqual():

    test_data : str
    expected_data : str
    
    def __post_init__(self):
        self.convert_to_pandas()
        self.get_column_set()
        self.reorder_data_frames()
        self.check_column_equality()
        self.check_data_equality()
    
    def convert_to_pandas(self):
        self.test_data = self.test_data.toPandas()
        self.expected_data = self.expected_data.toPandas()
        
    def reorder_data_frames(self):
        
        self.test_data = self.test_data[
            list(self.test_data_columns)
        ]
        self.expected_data = self.expected_data[
            list(self.expected_data_columns)
        ]
    
    def get_column_set(self):
        self.expected_data_columns = set(self.expected_data.columns)
        self.test_data_columns = set(self.expected_data.columns)
        
    def check_column_equality(self):
        assert self.expected_data_columns == self.test_data_columns
        
    
    def check_data_equality(self):
        assert (self.expected_data.values == self.test_data.values).all()
    
    
    