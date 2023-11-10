import unittest

from utils import (
    PySparkTestCase,
    AssertFramesEqual
)
from context import (
    DataPipelineQuery as queries
)
    
    
class TestDataPipeline(PySparkTestCase):
    """A class for testing data transformations
    underlying the `SparkifyDataPipeline` class. 
    """
    
    def test_user_last_level(self):
        """Tests query `DataPipelineQuery.GET_USER_LAST_LEVEL`
        using a mock dataset.
        
        Parameters
        ----------
        None 
        
        Returns
        -------
        None
        """
        mock_event_log = self.spark.createDataFrame([
            {'userId': '1', 'dt': '2018-05-01 20:00:00', 'level': 'paid'},
            {'userId': '1', 'dt': '2018-05-01 20:01:00', 'level': 'free'},
            {'userId': '1', 'dt': '2018-05-01 20:02:00', 'level': 'free'},
            {'userId': '2', 'dt': '2018-05-01 20:03:00', 'level': 'free'},
            {'userId': '2', 'dt': '2018-05-01 20:04:00', 'level': 'free'}
        ])
        
        query_results = self.spark.sql(
            queries.GET_USER_LAST_LEVEL, table=mock_event_log
        )
        
         
        expected_results = self.spark.createDataFrame([
            {'userId': '1', 'level': 'free'},
            {'userId': '2', 'level': 'free'}
        ])
        
        AssertFramesEqual(
            test_data=query_results, 
            expected_data=expected_results
        )
    
        
    def test_user_pages(self):
        """Tests query `DataPipelineQuery.GET_USER_PAGES`
        using a mock dataset.
        
        Parameters
        ----------
        None 
        
        Returns
        -------
        None
        """
        
        mock_event_log = self.spark.createDataFrame([
            {'userId': '1', 'gender': 'M', 'registration': '2018-05-01 20:00:00', 'page': 'NextSong', 'sessionId': 1},
            {'userId': '1', 'gender': 'M', 'registration': '2018-05-01 20:00:00', 'page': 'NextSong', 'sessionId': 1},
            {'userId': '1', 'gender': 'M', 'registration': '2018-05-01 20:00:00', 'page': 'Roll Advert', 'sessionId': 1},
            {'userId': '1', 'gender': 'M', 'registration': '2018-05-01 20:00:00', 'page': 'Save Settings', 'sessionId': 1},
            {'userId': '1', 'gender': 'M', 'registration': '2018-05-01 20:00:00', 'page': 'Home', 'sessionId': 1},
            {'userId': '1', 'gender': 'M', 'registration': '2018-05-01 20:00:00', 'page': 'Settings', 'sessionId': 1},
            {'userId': '1', 'gender': 'M', 'registration': '2018-05-01 20:00:00', 'page': 'About', 'sessionId': 1},
            {'userId': '1', 'gender': 'M', 'registration': '2018-05-01 20:00:00', 'page': 'Logout', 'sessionId': 1},
            {'userId': '1', 'gender': 'M', 'registration': '2018-05-01 20:00:00', 'page': 'Error', 'sessionId': 1},
            {'userId': '1', 'gender': 'M', 'registration': '2018-05-01 20:00:00', 'page': 'Help', 'sessionId': 2},
            {'userId': '1', 'gender': 'M', 'registration': '2018-05-01 20:00:00', 'page': 'Submit Downgrade', 'sessionId': 2},
            {'userId': '1', 'gender': 'M', 'registration': '2018-05-01 20:00:00', 'page': 'Submit Upgrade', 'sessionId': 2},
            {'userId': '1', 'gender': 'M', 'registration': '2018-05-01 20:00:00', 'page': 'Add to Playlist', 'sessionId': 2},
            {'userId': '1', 'gender': 'M', 'registration': '2018-05-01 20:00:00', 'page': 'Thumbs Up', 'sessionId': 2},
            {'userId': '1', 'gender': 'M', 'registration': '2018-05-01 20:00:00', 'page': 'Add Friend', 'sessionId': 2},
            {'userId': '1', 'gender': 'M', 'registration': '2018-05-01 20:00:00', 'page': 'Cancellation Confirmation', 'sessionId': 2}
        ])
        
        query_results = self.spark.sql(
            queries.GET_USER_PAGES, table=mock_event_log, 
        )
        
        expected_results = self.spark.createDataFrame([
            {'userId': '1', 'registration': '2018-05-01 20:00:00', 'gender': 'M', 'next_songs': 2, 
             'rolled_adverts': 1, 'save_settings': 1, 'home': 1, 'settings': 1, 
             'about': 1, 'logout': 1, 'errors': 1, 'help': 1, 'submitted_downgrades': 1,
             'submitted_upgrades': 1, 'downgrades': 0, 'upgrades': 0, 'thumbs_up': 1, 
             'thumbs_down': 0, 'add_to_playlist': 1, 'added_friends': 1, 'cancel': 1}
        ])
        

        AssertFramesEqual(
            test_data=query_results, 
            expected_data=expected_results
        )

    def test_user_sessions(self):
        """
        Tests query `DataPipelineQuery.GET_USER_SESSIONS`
        using a mock dataset.
        
        Parameters
        ----------
        None 
        
        Returns
        -------
        None
        """
        mock_event_log = self.spark.createDataFrame([
            {'userId': '1', 'length': 100, 'dt': '2018-05-01 20:00:00', 'sessionId': 1},
            {'userId': '1', 'length': 100, 'dt': '2018-05-01 22:00:00', 'sessionId': 1},
            {'userId': '1', 'length': 200, 'dt': '2018-05-02 20:00:00', 'sessionId': 2},
            {'userId': '1', 'dt': '2018-05-02 21:00:00', 'sessionId': 2},
            {'userId': '1', 'length': 50, 'dt': '2018-05-03 20:00:00', 'sessionId': 3},
            {'userId': '1', 'length': 50, 'dt': '2018-05-08 21:00:00', 'sessionId': 4},
            {'userId': '1', 'length': 50, 'dt': '2018-05-08 22:00:00', 'sessionId': 4},
            {'userId': '1', 'length': 100, 'dt': '2018-05-10 20:00:00', 'sessionId': 5},
            {'userId': '1', 'length': 100, 'dt': '2018-05-10 23:00:00', 'sessionId': 5}
        ])
        
        query_results = self.spark.sql(
            queries.GET_USER_SESSIONS, table=mock_event_log
        )
        
        expected_results = self.spark.createDataFrame([
            {'userId': '1', 'mean_session_hours': 1.4, 'median_session_hours': 1.0, 
             'total_session_hours': 7.0, 'total_next_song_length': 750}
        ])

        AssertFramesEqual(
            test_data=query_results, 
            expected_data=expected_results
        )
        
        
    def test_user_timelines(self):
        """Tests query `DataPipelineQuery.GET_USER_TIMELINES`
        using a mock dataset.
        
        Parameters
        ----------
        None 
        
        Returns
        -------
        None
        """
        
        mock_observation_start = '2023-08-01 20:00:00'
        mock_observation_end = '2023-08-30 20:00:00'
        
        mock_event_log = self.spark.createDataFrame([
            {'userId': '1', 'registration': '2023-07-01 20:00:00', 
             'page': 'Home', 'dt': '2023-08-01 20:00:00', 'sessionId': 1}, 
            {'userId': '1', 'registration': '2023-07-01 20:00:00', 
             'page': 'Home', 'dt': '2023-08-02 20:00:00', 'sessionId': 2}, 
            {'userId': '1', 'registration': '2023-07-01 20:00:00', 
             'page': 'NextSong', 'dt': '2023-08-03 20:00:00', 'sessionId': 2}, 
            {'userId': '1', 'registration': '2023-07-01 20:00:00', 
             'page': 'Cancellation Confirmation', 'dt': '2023-08-20 20:00:00', 'sessionId': 3}
        ])
        
        query_results = self.spark.sql(
            queries.GET_USER_TIMELINES, table=mock_event_log, 
            args = {
                'observation_start': mock_observation_start,
                'observation_end': mock_observation_end
            }
        )
        
        expected_results = self.spark.createDataFrame([
            {'userId': '1', 'days_observed': 19.00, 'unique_days_observed': 20,
             'days_registered': 50.0, 'min_user_dt': '2023-08-01 20:00:00',
             'max_user_dt': '2023-08-20 20:00:00', 'observation_start': '2023-08-01 20:00:00',
             'observation_end': '2023-08-20 20:00:00'}
        ])
        
        
        AssertFramesEqual(
            test_data=query_results, 
            expected_data=expected_results
        )
    
    
    def test_user_last_7d(self):
        """Tests query `DataPipelineQuery.GET_USER_LAST_7_DAYS`
        using a mock dataset.
        
        Parameters
        ----------
        None 
        
        Returns
        -------
        None
        """
        mock_event_log = self.spark.createDataFrame([
            {'userId': '1', 'dt': '2023-06-01 20:10:00', 'page': 'NextSong', 'sessionId': 1},
            {'userId': '1', 'dt': '2023-07-01 20:20:00', 'page': 'NextSong', 'sessionId': 2},
            {'userId': '1', 'dt': '2023-06-01 20:30:00', 'page': 'Roll Advert', 'sessionId': 1},
            {'userId': '1', 'dt': '2023-07-01 20:00:00', 'page': 'Save Settings', 'sessionId': 2},
            {'userId': '1', 'dt': '2023-07-01 20:00:00', 'page': 'Home', 'sessionId': 2},
            {'userId': '1', 'dt': '2023-07-01 20:00:00', 'page': 'Settings', 'sessionId': 2},
            {'userId': '1', 'dt': '2023-07-01 20:00:00', 'page': 'About', 'sessionId': 2},
            {'userId': '1', 'dt': '2023-07-01 20:00:00', 'page': 'Logout', 'sessionId': 2},
            {'userId': '1', 'dt': '2023-07-01 20:00:00', 'page': 'Error', 'sessionId': 2},
            {'userId': '1', 'dt': '2023-07-01 20:00:00', 'page': 'Help', 'sessionId': 2},
            {'userId': '1', 'dt': '2023-07-01 20:00:00', 'page': 'Submit Downgrade', 'sessionId': 2},
            {'userId': '1', 'dt': '2023-07-01 20:00:00', 'page': 'Submit Upgrade', 'sessionId': 2},
            {'userId': '1', 'dt': '2023-07-01 20:00:00', 'page': 'Add to Playlist', 'sessionId': 2},
            {'userId': '1', 'dt': '2023-07-01 20:00:00', 'page': 'Thumbs Up', 'sessionId': 2},
            {'userId': '1', 'dt': '2023-07-01 20:00:00', 'page': 'Add Friend', 'sessionId': 2},
            {'userId': '1', 'dt': '2023-07-03 20:00:00', 'page': 'Cancellation Confirmation', 'sessionId': 3}
        ])
        
        mock_user_datetimes = self.spark.createDataFrame([
            {'userId': '1', 'observation_end': '2023-07-03 20:00:00'}
        ])

        query_results = self.spark.sql(
            queries.GET_USER_LAST_7_DAYS, 
            table1=mock_event_log, 
            table2=mock_user_datetimes
        )

        expected_results = self.spark.createDataFrame([
            {'userId': '1', 'next_songs_last_7d': 1, 'rolled_adverts_last_7d': 0, 
             'save_settings_last_7d': 1, 'home_last_7d': 1, 'settings_last_7d': 1, 
             'about_last_7d': 1, 'logouts_last_7d': 1, 'errors_last_7d': 1, 'help_last_7d': 1, 
             'submitted_downgrades_last_7d': 1, 'submitted_upgrades_last_7d': 1, 'downgrades_last_7d': 0, 
             'upgrades_last_7d': 0, 'thumbs_up_last_7d': 1, 'thumbs_down_last_7d': 0, 'add_to_playlist_last_7d': 1, 
             'added_friends_last_7d': 1, 'unique_sessions_last_7d': 2}
        ])
        
        AssertFramesEqual(
            test_data=query_results, 
            expected_data=expected_results
        )
    
    
    def test_user_agents(self):
        """Tests query `DataPipelineQuery.PARSE_USER_AGENTS` 
        using a mock dataset.
        
        Parameters
        ----------
        None 
        
        Returns
        -------
        None
        """
        
        mock_event_log = self.spark.createDataFrame([
            {'userId': '1', 
             'userAgent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko/20100101 Firefox/31.0'
            }
        ])
        
        query_results = self.spark.sql(
            queries.PARSE_USER_AGENTS, table=mock_event_log
        )
        
        expected_results = self.spark.createDataFrame([
            {'userId': '1', 'is_mobile': False, 'os': 'Windows', 'browser': 'Firefox'}
        ])
        
        AssertFramesEqual(
            test_data=query_results, 
            expected_data=expected_results
        )
        
        
    def test_user_levels(self):
        """Tests query `DataPipelineQuery.GET_USER_DAYS_ACTIVE_BY_LEVEL` 
        using a mock dataset.
        
        Parameters
        ----------
        None 
        
        Returns
        -------
        None 
        """
        
        mock_event_log = self.spark.createDataFrame([
            {'userId': '1', 'dt': '2023-07-03 20:00:00', 'level': 'free', 'length': 100},
            {'userId': '1', 'dt': '2023-07-08 20:00:00', 'level': 'free', 'length': 100},
            {'userId': '1', 'dt': '2023-07-08 22:00:00', 'level': 'free', 'length': 100},
            {'userId': '1', 'dt': '2023-07-09 20:00:00', 'level': 'paid', 'length': 100},
            {'userId': '1', 'dt': '2023-07-15 20:00:00', 'level': 'paid', 'length': 100},
            {'userId': '1', 'dt': '2023-07-17 20:00:00', 'level': 'paid', 'length': 100}
        ])
        
        query_results = self.spark.sql(
            queries.GET_USER_DAYS_ACTIVE_BY_LEVEL, table=mock_event_log
        )
        
        expected_results = self.spark.createDataFrame([
            {'userId': '1', 'days_active': 5, 'free_days_active': 2, 'paid_days_active': 3, 'length': 600}
        ])
        
        AssertFramesEqual(
            test_data=query_results, 
            expected_data=expected_results
        )
        


if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=0, exit=True, warnings='ignore')