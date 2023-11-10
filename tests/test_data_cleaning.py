import unittest

from utils import ( 
    AssertFramesEqual,
    PySparkTestCase
)
from context import (
    DataCleaningQuery as queries, 
    DataCleaning
) 


class TestDataCleaning(PySparkTestCase):
    """A class for testing data transformations
    underlying the `SparkifyDataCleaning` class.
    """

    def test_drop_invalid_users(self):
        """Tests query `DataCleaningQuery.DROP_INVALID_USERS`
        using a mock dataset.
        
        Parameters
        ----------
        None 
        
        Returns
        -------
        None
        
        """
        mock_event_log = self.spark.createDataFrame([
            {'userId': '1', 'ts': '2018-05-01 20:00:00'},
            {'userId': '1', 'ts': '2018-05-01 20:01:00'},
            {'userId': '1', 'ts': '2018-05-01 20:02:00'},
            {'userId': '', 'ts': '2018-05-01 20:03:00'}
        ])
        
        query_results = self.spark.sql(
            queries.DROP_INVALID_USERS, 
            table=mock_event_log
        )
        
        expected_results = self.spark.createDataFrame([
            {'userId': '1', 'ts': '2018-05-01 20:00:00'},
            {'userId': '1', 'ts': '2018-05-01 20:01:00'},
            {'userId': '1', 'ts': '2018-05-01 20:02:00'}
        ])
    
        AssertFramesEqual(
            query_results, 
            expected_results
        )
    
    
    def test_drop_users_with_invalid_registration(self):
        """Tests query `DataCleaningQuery.DROP_USERS_WITH_INVALID_REGISTRATION`
        using a mock dataset.
        
        Parameters
        ----------
        None
        
        Returns
        -------
        None
        
        """
        mock_event_log = self.spark.createDataFrame([
            {'userId': '1', 'ts': 1699054228620, 'registration': 1699054228618},
            {'userId': '1', 'ts': 1699054228626, 'registration': 1699054228618},
            {'userId': '1', 'ts': 1699054228628, 'registration': 1699054228618},
            {'userId': '1', 'ts': 1699054228630, 'registration': 1699054228618},
            {'userId': '1', 'ts': 1699054228634, 'registration': 1699054228618},
            {'userId': '2', 'ts': 1699054228620, 'registration': 1699054228622},
            {'userId': '2', 'ts': 1699054228623, 'registration': 1699054228622},
            {'userId': '2', 'ts': 1699054228626, 'registration': 1699054228622},
            {'userId': '2', 'ts': 1699054228629, 'registration': 1699054228622},
            {'userId': '2', 'ts': 1699054228630, 'registration': 1699054228622}
        ])
        
        query_results = self.spark.sql(
            queries.DROP_USERS_WITH_INVALID_REGISTRATION, 
            table=mock_event_log
        )
        
        expected_results = self.spark.createDataFrame([
            {'userId': '1', 'ts': 1699054228620, 'registration': 1699054228618},
            {'userId': '1', 'ts': 1699054228626, 'registration': 1699054228618},
            {'userId': '1', 'ts': 1699054228628, 'registration': 1699054228618},
            {'userId': '1', 'ts': 1699054228630, 'registration': 1699054228618},
            {'userId': '1', 'ts': 1699054228634, 'registration': 1699054228618}
        ])
        
        AssertFramesEqual(
            query_results, 
            expected_results
        )
    
    def test_drop_users_with_null_registration(self):
        """Tests query `DataCleaningQuery.DROP_USERS_WITH_NULL_REGISTRATION`
        using a mock dataset.
        
        Parameters
        ----------
        None
        
        Returns
        -------
        None
        
        """
        mock_event_log = self.spark.createDataFrame([
            {'userId': '2'},
            {'userId': '3', 'registration': '2018-05-01 20:00:00'}
        ])
        
        query_results = self.spark.sql(
            queries.DROP_USERS_WITH_NULL_REGISTRATION, 
            table=mock_event_log
        )
        
        expected_results = self.spark.createDataFrame([
            {'userId': '3', 'registration': '2018-05-01 20:00:00'}
        ])
    
        AssertFramesEqual(
            query_results, 
            expected_results
        )
    
    
    def test_convert_ts_to_datetime(self):
        """Tests method `DataCleaning.convert_ts_to_datetime`
        using a mock dataset.
        
        Parameters
        ----------
        None
        
        Returns
        -------
        None
        
        """
        data_cleaning = DataCleaning(
            spark=self.spark
        )
        
        mock_event_log = self.spark.createDataFrame([
            {'userId': '1', 'ts': 1699054228626}
        ])
        
        query_results = data_cleaning.convert_ts_to_datetime(
            mock_event_log
        )
        
        expected_results = self.spark.createDataFrame([
            {'userId': '1', 'dt': '2023-11-03 17:30:28', 'ts': 1699054228626}
        ])
    
        AssertFramesEqual(
            query_results, 
            expected_results
        )
    
    
    def test_convert_registration_to_datetime(self):
        """Tests method `DataCleaning.convert_registration_to_datetime` 
        using a mock dataset.
        
        Parameters
        ----------
        None
        
        Returns
        -------
        None
        
        """
        
        data_cleaning = DataCleaning(
            spark=self.spark
        )
        
        mock_event_log = self.spark.createDataFrame([
            {'userId': '1', 'registration': 1699054228626},
        ])
        
        query_results = data_cleaning.convert_registration_to_datetime(
            mock_event_log
        )
        
        expected_results = self.spark.createDataFrame([
            {'userId': '1', 'registration': '2023-11-03 17:30:28'}
        ])
    
        AssertFramesEqual(
            query_results, 
            expected_results
        )
        
        

if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=0, exit=True, warnings='ignore')