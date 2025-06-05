import json
from dispycanoe.tile import Tile

def test_tile_initialization():
    """Test basic tile initialization."""
    features = [
        {
            "geometry": json.dumps({
                "type": "Point",
                "coordinates": [0, 0]
            }),
            "properties": json.dumps({"name": "test"})
        }
    ]
    
    tile = Tile(zoom=10, x=512, y=512, features=features)
    
    assert tile.zoom == 10
    assert tile.x == 512
    assert tile.y == 512
    assert len(tile.features) == 1

def test_tile_to_geojson():
    """Test conversion to GeoJSON."""
    features = [
        {
            "geometry": json.dumps({
                "type": "Point",
                "coordinates": [0, 0]
            }),
            "properties": json.dumps({"name": "test"})
        }
    ]
    
    tile = Tile(zoom=0, x=0, y=0, features=features)
    geojson = tile.to_geojson()
    
    assert geojson["type"] == "FeatureCollection"
    assert len(geojson["features"]) == 1
    assert geojson["features"][0]["type"] == "Feature"
    assert geojson["features"][0]["geometry"]["type"] == "Point"
    assert geojson["features"][0]["properties"]["name"] == "test" 