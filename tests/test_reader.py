"""
Tests for the reader module.
"""
from pathlib import Path
import json

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType

from dispycanoe.reader import GeoJSONReader

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return (SparkSession.builder
            .master("local[1]")
            .appName("dispycanoe-tests")
            .getOrCreate())

@pytest.fixture(scope="session")
def test_data_dir():
    """Get the path to the test data directory."""
    return Path(__file__).parent / "data"

@pytest.fixture
def reader(spark):
    """Create a GeoJSON reader instance."""
    return GeoJSONReader(spark)

def test_read_valid_geojson(reader, test_data_dir):
    """Test reading a valid GeoJSON file with point features."""
    # Read the populated places
    df = reader.read(test_data_dir / "populated_places.geojson")
    rows = df.collect()
    
    # Check for some major world cities
    cities = {
        "London": {"lon": -0.118, "lat": 51.501},
        "New York": {"lon": -73.981, "lat": 40.751},
        "Tokyo": {"lon": 139.749, "lat": 35.686}
    }
    
    for city_name, coords in cities.items():
        # Parse properties JSON string
        city = [r for r in rows if json.loads(r.properties)["NAME"] == city_name]
        assert len(city) == 1, f"Should find {city_name} in the data"
        city = city[0]
        assert city.longitude == pytest.approx(coords["lon"], rel=0.01)
        assert city.latitude == pytest.approx(coords["lat"], rel=0.01)

def test_schema_validation(reader, test_data_dir):
    """Test that the output DataFrame has the correct schema."""
    df = reader.read(test_data_dir / "populated_places.geojson")
    
    # Check column types
    assert isinstance(df.schema["latitude"].dataType, DoubleType)
    assert isinstance(df.schema["longitude"].dataType, DoubleType)
    assert isinstance(df.schema["properties"].dataType, StringType)
    
    # Verify we can parse the properties JSON
    sample = df.select("properties").first()
    props = json.loads(sample.properties)
    assert isinstance(props, dict)
    assert "NAME" in props

def test_invalid_geojson(reader, test_data_dir, tmp_path):
    """Test that invalid GeoJSON files raise appropriate errors."""
    # Create a non-FeatureCollection GeoJSON
    invalid_json = tmp_path / "invalid.geojson"
    invalid_json.write_text('{"type": "Feature", "geometry": null}')
    
    with pytest.raises(ValueError, match="must be a FeatureCollection"):
        reader.read(invalid_json)

def test_reader_multiline():
    """Test that multiline configuration works correctly."""
    spark = SparkSession.builder.master("local[1]").getOrCreate()
    
    # Default multiline
    reader = GeoJSONReader(spark)
    assert reader.multiline is True
    
    # Custom multiline
    reader = GeoJSONReader(spark, multiline=False)
    assert reader.multiline is False 