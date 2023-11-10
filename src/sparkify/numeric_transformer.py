from pyspark.ml.feature import (
    VectorAssembler
)




class NumericTransformer:
    """A class for assembling numeric features into a 
    vector and scaling the features.
    
    Parameters
    ----------
    columns : list[str]
        A list of columns to vectorize and scale. 
        
    scaler
        A scaler class from `pyspark.ml.feature`.
        
    vector_assembler_settings : dict
        Settings for the `pyspark.ml.feature.VectorAssembler` class.
        
    scaler_settings : dict
        Settings for the scaler.
    
    Attributes
    ----------
    columns : list[str]
        The list of columns to vectorize and scale.
        
    scaler
        A configured scaler class from `pyspark.ml.feature`.
    
    vector_assembler: pyspark.ml.feature.VectorAssembler
        A configured `pyspark.ml.feature.VectorAssembler`.

    output_column : list[str]
        A list containing the column output from
        `scaler`.
    
    """
    
    def __init__(
        self, 
        scaler,
        columns: list, 
        vector_assembler_settings: dict={},
        scaler_settings: dict={}
        
    ):
        self.columns = columns
        
        self.set_vector_assembler(
            settings=vector_assembler_settings
        )
        self.set_scaler(
            scaler=scaler, 
            settings=scaler_settings
        )
        self.set_output_column()
    
    
    def set_vector_assembler(self, settings):
        """Creates a `pyspark.ml.feature.VectorAssembler` for merging 
        multiple columns into a vector.
        
        Parameters
        ----------
        settings : dict
             Settings for the `VectorAssembler` class.
             
        Returns
        -------
        None
        
        """
        
        self.vector_assembler = VectorAssembler(
            inputCols=self.columns,
            outputCol='numeric_features',
            **settings
        )
        
    def set_scaler(self, scaler, settings):
        """Creates a scaler for scaling the numeric
        features in `vector_assembler`.
        
        Parameters
        ----------
        scaler
            A scaler class.
        
        settings : dict
            Settings for the scaler.
        
        Returns
        -------
        None
        
        """
        self.scaler = scaler(
            inputCol=self.vector_assembler.getOutputCol(),
            outputCol='scaled_features',
            **settings
        )
        
    def set_output_column(self):
        """Assigns the output columns `one_hot_encoder`
        to a class instance attribute `output_columns`.
        
        Parameters
        ----------
        None
        
        Returns
        -------
        None
        """
        
        self.output_column = [self.scaler.getOutputCol()]