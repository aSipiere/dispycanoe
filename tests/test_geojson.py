import json
import pytest
import tempfile
import os
from shapely.geometry import Point, mapping
from dispycanoe.geojson import load_geojson

def test_load_valid_geojson(spark):
    """Test loading a valid GeoJSON file."""
    # Create a temporary GeoJSON file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False) as f:
        features = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": mapping(Point(0, 0)),
                    "properties": {"name": "test1"}
                },
                {
                    "type": "Feature",
                    "geometry": mapping(Point(1, 1)),
                    "properties": {"name": "test2"}
                }
            ]
        }
        json.dump(features, f)
        filepath = f.name
    
    try:
        # Load the GeoJSON
        df = load_geojson(spark, filepath)
        
        # Check results
        rows = df.collect()
        assert len(rows) == 2
        assert all(row.geometry and row.properties for row in rows)
        
        # Check geometry format
        geom = json.loads(rows[0].geometry)
        assert geom["type"] == "Point"
        assert len(geom["coordinates"]) == 2
        
        # Check properties format
        props = json.loads(rows[0].properties)
        assert props["name"] in ["test1", "test2"]
    
    finally:
        os.unlink(filepath)

def test_load_invalid_geojson(spark):
    """Test loading invalid GeoJSON files."""
    # Test non-FeatureCollection
    with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False) as f:
        json.dump({"type": "Point", "coordinates": [0, 0]}, f)
        filepath = f.name
    
    try:
        with pytest.raises(ValueError, match="Input must be a GeoJSON FeatureCollection"):
            load_geojson(spark, filepath)
    finally:
        os.unlink(filepath)
    
    # Test empty FeatureCollection
    with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False) as f:
        json.dump({"type": "FeatureCollection", "features": []}, f)
        filepath = f.name
    
    try:
        with pytest.raises(ValueError, match="No features found in GeoJSON"):
            load_geojson(spark, filepath)
    finally:
        os.unlink(filepath)
    
    # Test invalid feature format
    with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False) as f:
        features = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "NotAFeature",
                    "geometry": mapping(Point(0, 0))
                }
            ]
        }
        json.dump(features, f)
        filepath = f.name
    
    try:
        with pytest.raises(ValueError, match="Invalid feature format"):
            load_geojson(spark, filepath)
    finally:
        os.unlink(filepath) 