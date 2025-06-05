import json
from shapely.geometry import shape
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def load_geojson(spark: SparkSession, input_path: str) -> DataFrame:
    """Load a GeoJSON file into a Spark DataFrame.
    
    Args:
        spark: Active Spark session
        input_path: Path to GeoJSON file
    
    Returns:
        DataFrame with geometry and properties columns
    """
    with open(input_path, 'r') as f:
        data = json.load(f)
    
    if not isinstance(data, dict) or data.get("type") != "FeatureCollection":
        raise ValueError("Input must be a GeoJSON FeatureCollection")
    
    features = data.get("features", [])
    if not features:
        raise ValueError("No features found in GeoJSON")
    
    # Convert features to rows, validating each one
    rows = []
    for feature in features:
        if not isinstance(feature, dict) or feature.get("type") != "Feature":
            raise ValueError("Invalid feature format")
        
        geometry = feature.get("geometry")
        if not isinstance(geometry, dict) or "type" not in geometry or "coordinates" not in geometry:
            raise ValueError("Invalid geometry format")
        
        if geometry["type"] not in ("Point", "MultiPoint"):
            raise ValueError("Only Point and MultiPoint geometries are supported")
        
        properties = feature.get("properties", {})
        if not isinstance(properties, dict):
            raise ValueError("Properties must be a dictionary")
        
        # Validate geometry by parsing it
        shape(geometry)
        
        rows.append((
            json.dumps(geometry),
            json.dumps(properties)
        ))
    
    schema = StructType([
        StructField("geometry", StringType(), False),
        StructField("properties", StringType(), False)
    ])
    
    return spark.createDataFrame(rows, schema) 