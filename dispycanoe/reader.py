"""
Reader implementations for various spatial data formats.
"""
from typing import Union
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from dispycanoe.models import Reader

class GeoJSONReader(Reader):
    """Reads point features from GeoJSON files."""
    
    def __init__(self, spark: SparkSession, multiline: bool = True):
        """
        Initialize the GeoJSON reader.
        
        Args:
            spark: Active SparkSession
            multiline: Set to True for pretty-printed GeoJSON where features span multiple lines.
                      Set to False for GeoJSON Lines format where each line is a complete feature.
                      (default: True)
        """
        super().__init__(spark)
        self.multiline = multiline
    
    def read(self, path: Union[str, Path]) -> DataFrame:
        """
        Read point features from a GeoJSON file.
        
        Args:
            path: Path to the GeoJSON file
            
        Returns:
            DataFrame with columns:
            - latitude: Double
            - longitude: Double
            - properties: String (JSON string containing all properties)
            
        Raises:
            ValueError: If the input is not a GeoJSON FeatureCollection
        """
        # Convert path to string for Spark
        str_path = str(path)
        
        # Read GeoJSON using Spark's JSON reader
        reader = self.spark.read
        if self.multiline:
            reader = reader.option("multiLine", True)
        raw_df = reader.json(str_path)
        
        # Ensure this is a FeatureCollection by checking if 'features' exists
        if 'features' not in raw_df.columns:
            raise ValueError("GeoJSON must be a FeatureCollection")
        
        # Explode the features array to get individual features
        features_df = raw_df.select(F.explode('features').alias('feature'))
        
        # Extract geometry and properties
        df = features_df.select(
            F.col('feature.geometry.coordinates').getItem(1).cast(DoubleType()).alias('latitude'),
            F.col('feature.geometry.coordinates').getItem(0).cast(DoubleType()).alias('longitude'),
            F.to_json('feature.properties').alias('properties')  # Store as JSON string
        )
        
        return df