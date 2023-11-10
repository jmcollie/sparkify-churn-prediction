from .data_cleaning_query import DataCleaningQuery
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import col


class DataCleaning():
    """A class used to clean the Sparkify event_log.
    
    Parameters
    ----------
    spark : pyspark.sql.session.SparkSession
        A spark session.
        
    queries : DataCleaningQuery
        Queries for cleaning the event data.
        
    Attributes
    ----------
    spark : pyspark.sql.session.SparkSession
        A spark session.
        
    queries : DataCleaningQuery
        An instance of the `DataCleaningQuery` class for
        cleaning the event data.
    
    """
    def __init__(self, spark):
        self.queries = DataCleaningQuery()
        self.spark = spark
    
    
    def drop_invalid_users(self, event_log):
        """Drop users from `event_log` with a null or empty value
        in the ``userId`` column.
        
        Parameters
        ----------
        event_log : pyspark.sql.dataframe.DataFrame
            A spark DataFrame of user events.
        
        Returns
        -------
        : pyspark.sql.dataframe.DataFrame
            A spark Dataframe of user events with rows
            removed where ``userId`` column is null or empty.
        """
        
        event_log_temp = self.spark.sql(
            self.queries.DROP_INVALID_USERS, 
            table=event_log
        )
        
        return event_log_temp
        
    
    def drop_users_with_invalid_registration(self, event_log):
        """Drops users from `event_log` with an invalid registration date.
        
        Parameters
        ----------
        event_log : pyspark.sql.dataframe.DataFrame
            A spark DataFrame of user events.
        
        Returns
        -------
        : pyspark.sql.dataframe.DataFrame
            A spark Dataframe of user events with rows
            removed where ``registration`` column is invalid.
        """

        event_log_temp = self.spark.sql(
            self.queries.DROP_USERS_WITH_INVALID_REGISTRATION,
            table=event_log
        )

        return event_log_temp
        
        
    def drop_users_with_null_registration(self, event_log):
        """Drops users from `event_log` with a null 
        ``registration`` date.
        
        Parameters
        ----------
        event_log : pyspark.sql.dataframe.DataFrame
            A spark DataFrame of user events.
        
        Returns
        -------
        : pyspark.sql.dataframe.DataFrame
            A spark Dataframe of user events with rows
            removed where ``registration`` column is null.
        """
        
        event_log_temp = self.spark.sql(
            self.queries.DROP_USERS_WITH_NULL_REGISTRATION,
            table=event_log
        )
        
        return event_log_temp

    
    def convert_ts_to_datetime(self, event_log):
        """Converts ``ts`` column from timestamp to datetime
        and assigns the results to a new column ``dt``.
        
        Parameters
        ----------
        event_log : pyspark.sql.dataframe.DataFrame
            A spark DataFrame of user events.
            
        Returns
        -------
        : pyspark.sql.dataframe.DataFrame
            A spark Dataframe of user events with an
            added column ``dt``.
        """
        
        return event_log.withColumn(
            'dt', from_unixtime(col('ts')/1000)
        )
        
        
    def convert_registration_to_datetime(self, event_log):
        """Converts the ``registration`` column in `event_log` from timestamp to
        datetime and assigns the results back to the ``registration``
        column.
        
        Parameters
        ----------
        event_log : pyspark.sql.dataframe.DataFrame
            A spark DataFrame of user events.
            
        Returns
        -------
        : pyspark.sql.dataframe.DataFrame
            A spark Dataframe of user events with 
            ``registration`` column converted to datetime.
        """

        return event_log.withColumn(
            'registration', from_unixtime(col('registration')/1000)
        )
    