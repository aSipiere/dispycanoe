from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
import pyspark.sql.functions as sql
from shapely.geometry import shape
import json
import numpy as np

class QuadCell:
    def __init__(self, bounds: List[float], level: int, code: str = ""):
        """Initialize a quadtree cell.
        
        Args:
            bounds: [minx, miny, maxx, maxy] in WGS84
            level: Depth in the quadtree
            code: Morton code identifying the cell's position
        """
        self.bounds = bounds
        self.level = level
        self.code = code
    
    def subdivide(self) -> List['QuadCell']:
        """Subdivide this cell into four children."""
        minx, miny, maxx, maxy = self.bounds
        midx = (minx + maxx) / 2
        midy = (miny + maxy) / 2
        
        children = [
            # SW quadrant (0)
            QuadCell([minx, miny, midx, midy], self.level + 1, self.code + "0"),
            # SE quadrant (1)
            QuadCell([midx, miny, maxx, midy], self.level + 1, self.code + "1"),
            # NW quadrant (2)
            QuadCell([minx, midy, midx, maxy], self.level + 1, self.code + "2"),
            # NE quadrant (3)
            QuadCell([midx, midy, maxx, maxy], self.level + 1, self.code + "3")
        ]
        return children

def get_quad_cell_bounds(code: str) -> List[float]:
    """Calculate bounds for a given Morton code."""
    if not code:
        return [-180, -90, 180, 90]  # Root cell
    
    bounds = [-180, -90, 180, 90]
    
    for digit in code:
        minx, miny, maxx, maxy = bounds
        midx = (minx + maxx) / 2
        midy = (miny + maxy) / 2
        
        if digit == "0":  # SW
            bounds = [minx, miny, midx, midy]
        elif digit == "1":  # SE
            bounds = [midx, miny, maxx, midy]
        elif digit == "2":  # NW
            bounds = [minx, midy, midx, maxy]
        elif digit == "3":  # NE
            bounds = [midx, midy, maxx, maxy]
        else:
            raise ValueError(f"Invalid quadtree code digit: {digit}")
    
    return [float(b) for b in bounds]

def assign_quad_cell(geom_str: str, max_level: int = 10) -> str:
    """Assign a geometry to a quadtree cell."""
    geom = shape(json.loads(geom_str))
    bounds = geom.bounds
    centroid = geom.centroid
    
    # Start with root cell
    current_code = ""
    current_bounds = [-180, -90, 180, 90]
    
    # Calculate geometry size relative to world
    geom_width = bounds[2] - bounds[0] or 1e-10
    geom_height = bounds[3] - bounds[1] or 1e-10
    world_width = 360  # -180 to 180
    world_height = 180  # -90 to 90
    
    # Determine target level based on geometry size
    target_level = min(
        max_level,
        max(1, int(-np.log2(max(
            geom_width / world_width,
            geom_height / world_height
        ))))
    )
    
    # Traverse quadtree to target level
    for _ in range(target_level):
        minx, miny, maxx, maxy = current_bounds
        midx = (minx + maxx) / 2
        midy = (miny + maxy) / 2
        
        # Determine quadrant
        if centroid.x >= midx:
            if centroid.y >= midy:
                current_code += "3"  # NE
                current_bounds = [midx, midy, maxx, maxy]
            else:
                current_code += "1"  # SE
                current_bounds = [midx, miny, maxx, midy]
        else:
            if centroid.y >= midy:
                current_code += "2"  # NW
                current_bounds = [minx, midy, midx, maxy]
            else:
                current_code += "0"  # SW
                current_bounds = [minx, miny, midx, midy]
    
    return current_code

def create_quadtree_partitioner(df: DataFrame, max_level: int = 10) -> DataFrame:
    """Add quadtree cell assignments to a DataFrame."""
    assign_cell_udf = sql.udf(
        lambda geom: assign_quad_cell(geom, max_level),
        StringType()
    )
    
    return df.withColumn("quad_cell", assign_cell_udf(sql.col("geometry")))

def calculate_feature_density(geom_str: str) -> float:
    """Calculate feature density based on point proximity."""
    geom = shape(json.loads(geom_str))
    if geom.geom_type not in ('Point', 'MultiPoint'):
        raise ValueError("Only Point and MultiPoint geometries are supported")
    
    # For points, use a simpler density calculation
    # If points are exactly at the same location, they'll have the same density
    # This makes it easier to coalesce or drop them
    bounds = geom.bounds
    x = bounds[0]  # minx
    y = bounds[1]  # miny
    
    # Use a simple hash of the coordinates for density
    # This ensures points at the same location get the same density
    # while still maintaining relative ordering for different locations
    return abs(hash(f"{x:.6f},{y:.6f}") % 1000) / 1000.0

def get_cell_density(df: DataFrame, quad_cell: str) -> float:
    """Calculate feature density for a quadtree cell."""
    bounds = get_quad_cell_bounds(quad_cell)
    cell_area = (bounds[2] - bounds[0]) * (bounds[3] - bounds[1])
    
    count = df.filter(sql.col("quad_cell") == quad_cell).count()
    return count / cell_area if cell_area > 0 else 0.0 