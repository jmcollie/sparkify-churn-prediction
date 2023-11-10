from .data_pipeline_query import DataPipelineQuery


class DataPipeline():
    """A class for transforming the Sparkify event log 
    into user features.
    
    Parameters
    ----------
    spark : pyspark.sql.session.SparkSession
        A spark session.
    queries : DataPipelineQuery
        Queries for transforming the event data
        into features.
        
    Attributes
    ----------
    spark : pyspark.sql.session.SparkSession
        A spark session.
    queries : DataPipelineQuery
        Queries for transforming the event data
        into features.
    """
    def __init__(self, spark):
        
        self.queries = DataPipelineQuery()
        self.spark = spark
        
    
    def get_observation_period(self, event_log):
        """Gets the user observation period from `event_log`.
        
        Parameters
        ----------
        event_log : pyspark.sql.dataframe.DataFrame
            A spark DataFrame of user events.
        
        Returns
        -------
        observation_start
            Start datetime of the observation period.
        observation_end
            End datetime of the observation period.
        """
        observation_start, observation_end = self.spark.sql(
            self.queries.GET_USER_OBSERVATION_PERIOD,
            table=event_log
        ).collect()[0]
        
        return observation_start, observation_end
    
    
    def get_user_pages(self, event_log):
        """Gets a count of user page visits from `event_log`.
        
        Parameters
        ----------
        event_log : pyspark.sql.dataframe.DataFrame
            A spark DataFrame of user events.
        
        Returns
        -------
        : pyspark.sql.dataframe.DataFrame
            A spark DataFrame of user page visits.
        """
        
        return self.spark.sql(
            self.queries.GET_USER_PAGES, 
            table=event_log
        )
        
        
    def get_user_sessions(self, event_log):
        """Aggregates user session data from `event_log`.
        
        Parameters
        ----------
        event_log : pyspark.sql.dataframe.DataFrame
            A spark DataFrame of user events.
            
        Returns
        -------
        : pyspark.sql.dataframe.DataFrame
            A spark DataFrame of user session data.
        """
        return self.spark.sql(
            self.queries.GET_USER_SESSIONS, 
            table=event_log
        )
    
    
    def get_user_timelines(self, event_log, observation_start, observation_end):
        """Gets user timeline data such as the number of days the user has been
        registered, how long the user has been observed, etc. from
        `event_log`.
        
        Parameters
        ----------
        event_log : pyspark.sql.dataframe.DataFrame
            A spark DataFrame of user events.
        observation_start
            Start datetime of the observation period.
        observation_end
            End datetime of the observation period.
            
        Returns
        -------
        : pyspark.sql.dataframe.DataFrame
            A spark DataFrame of user timeline data.
        """
        return self.spark.sql(
            self.queries.GET_USER_TIMELINES,
            {
                'observation_start': observation_start,
                'observation_end': observation_end
            },
            table=event_log
        )
    
    
    def get_user_last_level(self, event_log):
        """Gets the user last level from the ``level``
        column in `event_log`.
        
        Parameters
        ----------
        event_log : pypsark.sql.dataframe.DataFrame
            A spark DataFrame of user events.
            
        Returns
        -------
        : pyspark.sql.dataframe.DataFrame
            A DataFrame of users' latest level value
            from the ``level`` column.
        """
        return self.spark.sql(
            self.queries.GET_USER_LAST_LEVEL,
            table=event_log
        )
    
    def get_user_days_active_by_level(self, event_log):
        """Gets the number of days active 
        for each user level.
        
        Parameters
        ----------
        event_log : pypsark.sql.dataframe.DataFrame
            A spark DataFrame of user events.
            
        Returns
        -------
        : pyspark.sql.dataframe.DataFrame
            A DataFrame of `user_levels`
        """
        return self.spark.sql(
            self.queries.GET_USER_DAYS_ACTIVE_BY_LEVEL,
            table=event_log
        )
        
    def parse_user_agents(self, event_log):
        """Parses the ``userAgent`` column in `event_log`
        and creates features from the parsed values.
        
        Parameters
        ----------
        event_log : pypsark.sql.dataframe.DataFrame
            A spark DataFrame of user events.
            
        Returns
        -------
        : pyspark.sql.dataframe.DataFrame
            A DataFrame of values parsed from ``userAgent`` column
            in `event_log`.
        """
        return self.spark.sql(
            self.queries.PARSE_USER_AGENTS,
            table=event_log
        )
    
    
    def parse_user_locations(self, event_log):
        """Parses the state from the ``location`` column 
        in `event_log`.
        
        Parameters
        ----------
        event_log : pypsark.sql.dataframe.DataFrame
            A spark DataFrame of user events.
            
        Returns
        -------
        : pyspark.sql.dataframe.DataFrame
            A spark DataFrame of parsed user location data.
        
        """
        return self.spark.sql(
            self.queries.PARSE_USER_LOCATIONS,
            table=event_log
        )
        
    
    def get_user_last_7_days(self, event_log, user_datetimes):
        """Gets users' last 7 days of ``page`` visit data
        from `event_log`.
        
        Parameters
        ----------
        event_log_table : pypsark.sql.dataframe.DataFrame
            A spark DataFrame of user events.
            
        Returns
        -------
        : pyspark.sql.dataframe.DataFrame
            A DataFrame of users' last 7 days of page visits .
        """
        return self.spark.sql(
            self.queries.GET_USER_LAST_7_DAYS,
            table1=event_log,
            table2=user_datetimes
        )
    
    
    def run(self, event_log):
        """Runs queries for transforming the `event_log`
        into user features.
        
        Parameters
        ----------
        event_log : pypsark.sql.dataframe.DataFrame
            A spark DataFrame of user events.
        
        Returns
        -------
        user_data : pyspark.sql.dataframe.DataFrame
            A dataframe of user features extracted and transformed
            from columns in `event_log`.
        """
        # Getting the observation start and end datetimes.
        observation_start, observation_end = self.get_observation_period(
            event_log
        )
        
        # Creating Spark DataFrames with features extracted from
        # the user event log data.
        user_pages = self.get_user_pages(event_log)
        
        user_sessions = self.get_user_sessions(event_log)
        
        user_timelines = self.get_user_timelines(
            event_log, 
            observation_start, 
            observation_end
        )

        user_last_level = self.get_user_last_level(event_log)
        user_days_active_by_level = self.get_user_days_active_by_level(
            event_log
        ) 
        parsed_user_agents = self.parse_user_agents(event_log)
        parsed_user_locations = self.parse_user_locations(event_log)
        
        user_last_7_days = self.get_user_last_7_days(
            event_log,
            user_timelines
        )
        
        
        # Creating a view using each of the DataFrames.
        user_data = self.spark.sql(
            self.queries.GET_USER_EVENTS, 
            {
                'observation_start': observation_start,
                'observation_end' : observation_end
            },
            user_pages=user_pages,
            parsed_user_agents=parsed_user_agents,
            user_sessions=user_sessions,
            user_timelines=user_timelines,
            user_last_level=user_last_level,
            user_days_active_by_level=user_days_active_by_level,
            parsed_user_locations=parsed_user_locations,
            user_last_7_days=user_last_7_days 
        )
        
        return user_data