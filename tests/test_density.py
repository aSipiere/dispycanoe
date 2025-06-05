import pytest
from pyspark.sql import SparkSession
from dispycanoe.density import reduce_density
from dispycanoe.spatial import create_quadtree_partitioner
import json
from shapely.geometry import Point, mapping

def test_reduce_density_drop(spark):
    """Test density reduction by dropping features."""
    # Create minimal test data with a small dense cluster
    features = []
    
    # Dense cluster - just 6 points in a very tight group (0.001 unit spacing)
    for i in range(6):
        point = Point(0 + (i % 2) * 0.001, 0 + (i // 2) * 0.001)
        features.append((
            json.dumps(mapping(point)),
            json.dumps({"id": f"dense_{i}"})
        ))
    
    # Two scattered points
    features.append((
        json.dumps(mapping(Point(1, 1))),
        json.dumps({"id": "scattered_1"})
    ))
    features.append((
        json.dumps(mapping(Point(-1, -1))),
        json.dumps({"id": "scattered_2"})
    ))
    
    # Create DataFrame
    schema = "geometry STRING, properties STRING"
    df = spark.createDataFrame(features, schema)
    df = create_quadtree_partitioner(df)
    
    # Test dropping densest features
    reduced_df = reduce_density(
        df,
        [-2, -2, 2, 2],  # Small bounds
        feature_limit=4,  # Want to keep scattered points and some dense ones
        drop_densest=True,
        coalesce_densest=False,
        drop_fraction=0.5  # Drop half of excess each iteration
    )
    
    # Check results
    final_count = reduced_df.count()
    assert final_count <= 4
    assert final_count >= 2  # Should keep at least scattered points

def test_reduce_density_coalesce(spark):
    """Test density reduction by coalescing features."""
    # Create minimal test data - just 4 points in a very tight group
    features = []
    
    # Dense cluster (0.001 unit spacing)
    for i in range(4):
        point = Point(0 + (i % 2) * 0.001, 0 + (i // 2) * 0.001)
        features.append((
            json.dumps(mapping(point)),
            json.dumps({"id": f"dense_{i}"})
        ))
    
    # Create DataFrame
    schema = "geometry STRING, properties STRING"
    df = spark.createDataFrame(features, schema)
    df = create_quadtree_partitioner(df)
    
    # Test coalescing dense features
    coalesced_df = reduce_density(
        df,
        [-2, -2, 2, 2],  # Small bounds
        feature_limit=2,
        drop_densest=False,
        coalesce_densest=True,
        coalesce_fraction=1.0,  # Coalesce all excess points
        min_points_to_coalesce=2  # Minimum cluster size
    )
    
    # Check results
    result = coalesced_df.collect()
    has_coalesced = any(
        json.loads(row.properties).get("coalesced", False)
        for row in result
    )
    assert has_coalesced
    assert coalesced_df.count() <= 2

def test_no_reduction_needed(spark):
    """Test when density reduction is not needed."""
    # Create minimal test data - just 2 well-spaced points
    features = [
        (
            json.dumps(mapping(Point(-1, -1))),
            json.dumps({"id": "point_1"})
        ),
        (
            json.dumps(mapping(Point(1, 1))),
            json.dumps({"id": "point_2"})
        )
    ]
    
    # Create DataFrame
    schema = "geometry STRING, properties STRING"
    df = spark.createDataFrame(features, schema)
    df = create_quadtree_partitioner(df)
    
    # Test with feature limit above current count
    result_df = reduce_density(
        df,
        [-2, -2, 2, 2],  # Small bounds
        feature_limit=3,
        drop_densest=True
    )
    
    # Check no reduction occurred
    assert result_df.count() == df.count() 