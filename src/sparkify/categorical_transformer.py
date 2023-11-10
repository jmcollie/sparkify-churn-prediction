from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder
)


class CategoricalTransformer:
    """A class for transforming categorical columns
    into one hot encoded columns.
    
    Parameters
    ----------
    columns : list[str]
        A list of categorical columns to One-Hot encode.
        
    string_indexer_settings : dict
        Settings passed to the `pyspark.ml.feature.StringIndexer` class.
        
    one_hot_encoder_settings : dict
        Settings passed to the `pyspark.ml.feature.OneHotEncoder` class.
        
    Attributes
    ----------
    columns : list[str]
        A list of categorical columns to One-Hot encode.
        
    string_indexer: pyspark.ml.feature.StringIndexer
        A configured `pyspark.ml.feature.StringIndexer`.
    
    one_hot_encoder: pyspark.ml.feature.OneHotEncoder
        A configured `pyspark.ml.feature.OneHotEncoder`.
    
    output_columns : list[str]
        A list containing the columns output from
        `one_hot_encoder`.
    
    """
    
    def __init__(
        self, 
        columns: list, 
        string_indexer_settings: dict={},
        one_hot_encoder_settings: dict={}
    ):
        self.columns = columns
        
        self.set_string_indexer(
            string_indexer_settings
        )
        self.set_one_hot_encoder(
            one_hot_encoder_settings
        )
        self.set_output_columns()
        
        
    def set_string_indexer(self, settings: dict):
        """Maps a string column of labels to a column 
        of label indices and assigns the results
        to `string_indexer`.
        
        Parameters
        ----------
        settings : dict
            Settings for the `StringIndexer` class.
        
        Returns
        -------
        None
        
        """
        self.string_indexer = StringIndexer(
            inputCols=self.columns,
            outputCols=[
                column + '_idx' for
                column in self.columns
            ],
            **settings
        )


        
    def set_one_hot_encoder(self, settings: dict):
        """Creates a `OneHotEncoder` and assigns to 
        the result to a class attribute `one_hot_encoder`.
        
        Parameters
        ----------
        settings : dict
            Settings for the `OneHotEncoder` class.
        
        Returns
        -------
        None
        
        """
        self.one_hot_encoder = OneHotEncoder(
            inputCols=self.string_indexer.getOutputCols(),
            outputCols=[
                column + '_ohe' for column in 
                self.string_indexer.getOutputCols()
            ],
            **settings
        )
        
    def set_output_columns(self):
        """Creates a class attribute `output_columns` with and 
        assigns the output columns from the `one_hot_encoder` 
        class attribute.        
        
        Parameters
        ----------
        None
        
        Returns
        -------
        None
        
        """
        
        self.output_columns = self.one_hot_encoder.getOutputCols()