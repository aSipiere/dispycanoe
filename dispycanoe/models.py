"""
Core models and abstract base classes for spatial data handling.

This module defines the interfaces for reading spatial data
and building spatial indexes/tiles.
"""
from abc import ABC, abstractmethod
from typing import Union
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType

class Reader(ABC):
    """Abstract base class for reading spatial data into Spark DataFrames."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize the reader.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
    
    @abstractmethod
    def read(self, path: Union[str, Path]) -> DataFrame:
        """
        Read spatial data from the given path.
        
        Args:
            path: Path to the data file
            
        Returns:
            DataFrame with standardized spatial columns:
            - latitude: Double
            - longitude: Double
            - properties: Map containing source-specific properties
        """
        pass

class DensitySolver(ABC):
    """Abstract base class for finding dense point regions."""
    
    @abstractmethod
    def solve(self, df: DataFrame, zoom: int) -> DataFrame:
        """
        Find dense regions in the point data.
        
        Args:
            df: DataFrame with point data
            zoom: Current zoom level
            
        Returns:
            DataFrame with density information:
            - Original columns
            - density: Double (density score, higher means denser)
            - cluster_id: Long (optional, for clustered approaches)
        """
        pass
    
    def _validate_output(self, df: DataFrame) -> None:
        """
        Validate that the output DataFrame has the required density column.
        
        Args:
            df: DataFrame to validate
            
        Raises:
            ValueError: If density column is missing or has wrong type
        """
        if 'density' not in df.columns:
            raise ValueError("Solver must add a 'density' column")
        
        density_type = df.schema['density'].dataType
        if not isinstance(density_type, DoubleType):
            raise ValueError("'density' column must be of type Double")

class Reducer(ABC):
    """Abstract base class for reducing point density."""
    
    def _validate_input(self, df: DataFrame) -> None:
        """
        Validate that the input DataFrame has the required density column.
        
        Args:
            df: DataFrame to validate
            
        Raises:
            ValueError: If density column is missing or has wrong type
        """
        if 'density' not in df.columns:
            raise ValueError("Input DataFrame must have a 'density' column")
        
        density_type = df.schema['density'].dataType
        if not isinstance(density_type, DoubleType):
            raise ValueError("'density' column must be of type Double")
    
    @abstractmethod
    def reduce(self, df: DataFrame, zoom: int) -> DataFrame:
        """
        Reduce point density based on zoom level and density information.
        
        Args:
            df: DataFrame with density information
                Must have a 'density' column of type Double
            zoom: Current zoom level
            
        Returns:
            DataFrame with reduced points:
            - Original columns
            - is_retained: Boolean (whether point should be kept)
        """
        pass

class Writer(ABC):
    """Abstract base class for writing spatial data."""
    
    @abstractmethod
    def write(self, df: DataFrame, output_path: str) -> None:
        """
        Write spatial data to the specified output path.
        
        Args:
            df: DataFrame with features to write
            output_path: Path to write data to
        """
        pass

class Builder(ABC):
    """Abstract base class for building spatial data outputs."""
    
    def __init__(
        self,
        points_df: DataFrame,
        density_solver: DensitySolver,
        reducer: Reducer,
        writer: Writer
    ):
        """
        Initialize the builder.
        
        Args:
            points_df: Spark DataFrame with point data
            density_solver: Component for finding dense regions
            reducer: Component for reducing point density
            writer: Component for writing output
        """
        self._validate_input(points_df)
        self.points_df = points_df
        self.density_solver = density_solver
        self.reducer = reducer
        self.writer = writer
    
    def _validate_input(self, df: DataFrame) -> None:
        """
        Validate the input DataFrame has required columns.
        
        Args:
            df: DataFrame to validate
            
        Raises:
            ValueError: If required columns are missing
        """
        required_cols = {'latitude', 'longitude', 'properties'}
        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            raise ValueError(f"DataFrame missing required columns: {missing_cols}")
    
    @abstractmethod
    def build(self, output_path: str) -> None:
        """
        Build the spatial data output.
        
        Args:
            output_path: Path where the output will be written
        """
        pass 