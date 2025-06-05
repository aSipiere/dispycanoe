from typing import List, Dict, Any
import json

class Tile:
    """A vector tile representation."""
    
    def __init__(self, zoom: int, x: int, y: int, features: List[Dict[str, Any]]):
        """Initialize a vector tile.
        
        Args:
            zoom: Zoom level
            x: Tile x coordinate
            y: Tile y coordinate
            features: List of GeoJSON features in the tile
        """
        self.zoom = zoom
        self.x = x
        self.y = y
        self.features = features
    
    @property
    def id(self) -> str:
        """Get the tile ID in z/x/y format."""
        return f"{self.zoom}/{self.x}/{self.y}"
    
    def to_geojson(self) -> Dict[str, Any]:
        """Convert tile features to a GeoJSON FeatureCollection."""
        return {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": json.loads(feature["geometry"]) if isinstance(feature["geometry"], str) else feature["geometry"],
                    "properties": json.loads(feature["properties"]) if isinstance(feature["properties"], str) else feature["properties"]
                }
                for feature in self.features
            ]
        }
    
    def __len__(self) -> int:
        """Get the number of features in the tile."""
        return len(self.features)
    
    def __bool__(self) -> bool:
        """Check if the tile has any features."""
        return bool(self.features) 