from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from shapely.geometry import shape, mapping
from shapely.ops import unary_union
import json
from .spatial import calculate_feature_density

def reduce_density(
    df: DataFrame,
    bounds: List[float],
    feature_limit: int,
    drop_densest: bool = False,
    coalesce_densest: bool = False,
    drop_fraction: float = 0.75,
    coalesce_fraction: float = 0.6,
    min_points_to_coalesce: int = 4
) -> DataFrame:
    """Reduce feature density in a DataFrame using either dropping or coalescing."""
    if not (drop_densest or coalesce_densest):
        return df
    
    # Calculate feature density
    density_udf = F.udf(calculate_feature_density, FloatType())
    df = df.withColumn("density", density_udf(F.col("geometry")))
    
    # If we're under the feature limit, return as is
    if df.count() <= feature_limit:
        return df
    
    if drop_densest:
        return _drop_densest_features(df, feature_limit, drop_fraction)
    else:
        return _coalesce_densest_features(df, feature_limit, coalesce_fraction, min_points_to_coalesce)

def _drop_densest_features(df: DataFrame, feature_limit: int, drop_fraction: float) -> DataFrame:
    """Drop features from densest areas until under feature limit."""
    while df.count() > feature_limit:
        # Get current count
        total = df.count()
        to_drop = int((total - feature_limit) * drop_fraction)
        
        # Order by density and drop densest features
        df = df.orderBy(F.col("density").desc()) \
            .limit(total - to_drop)
    
    return df

def _coalesce_densest_features(
    df: DataFrame,
    feature_limit: int,
    coalesce_fraction: float,
    min_points_to_coalesce: int
) -> DataFrame:
    """Coalesce features in dense areas into representative points."""
    
    def coalesce_points(points: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Coalesce a group of points into a single representative point."""
        if len(points) < min_points_to_coalesce:
            return None
        
        # Parse geometries
        geoms = [shape(json.loads(p["geometry"])) for p in points]
        
        # Create union of points
        union = unary_union(geoms)
        centroid = union.centroid
        
        # Create coalesced feature
        properties = {
            "coalesced": True,
            "point_count": len(points),
            "original_properties": [json.loads(p["properties"]) for p in points]
        }
        
        return {
            "geometry": json.dumps(mapping(centroid)),
            "properties": json.dumps(properties),
            "density": 1.0  # Reset density for coalesced point
        }
    
    while df.count() > feature_limit:
        # Get current count
        total = df.count()
        to_coalesce = int((total - feature_limit) * coalesce_fraction)
        
        # Get densest features to coalesce
        densest = df.orderBy(F.col("density").desc()) \
            .limit(to_coalesce) \
            .collect()
        
        # Group by quad cell and coalesce
        by_cell = {}
        for row in densest:
            cell = row["quad_cell"]
            by_cell.setdefault(cell, []).append(row.asDict())
        
        # Create coalesced features
        coalesced = []
        for cell, points in by_cell.items():
            result = coalesce_points(points)
            if result:
                result["quad_cell"] = cell
                coalesced.append(result)
        
        # Remove original features and add coalesced ones
        if coalesced:
            dense_cells = list(by_cell.keys())
            df = df.filter(~F.col("quad_cell").isin(dense_cells))
            
            coalesced_df = df.sparkSession.createDataFrame(coalesced)
            df = df.union(coalesced_df)
        else:
            break  # No more features to coalesce
    
    return df 