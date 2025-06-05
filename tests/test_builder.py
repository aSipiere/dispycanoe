import pytest
import os
import json
from pyspark.sql import SparkSession

from dispycanoe import DisPyCanoeBuilder
from dispycanoe.spatial import create_quadtree_partitioner

def test_builder_init():
    """Test DisPyCanoeBuilder initialization with default parameters."""
    spark = SparkSession.builder.getOrCreate()
    
    builder = DisPyCanoeBuilder(
        spark=spark,
        max_zoom=14,
        min_zoom=0,
        tile_size=512,
        buffer_size=64,
        max_points_per_tile=100000,
        min_points_per_tile=1000,
    )
    
    assert builder.max_zoom == 14
    assert builder.min_zoom == 0
    assert builder.tile_size == 512
    assert builder.buffer_size == 64
    assert builder.max_points_per_tile == 100000
    assert builder.min_points_per_tile == 1000

def test_builder_invalid_params():
    """Test DisPyCanoeBuilder initialization with invalid parameters."""
    spark = SparkSession.builder.getOrCreate()
    
    with pytest.raises(ValueError):
        builder = DisPyCanoeBuilder(
            spark=spark,
            max_zoom=14,
            min_zoom=15,  # min_zoom > max_zoom
        )

def test_builder_drop_and_coalesce():
    """Test that builder raises error when both drop and coalesce are enabled."""
    spark = SparkSession.builder.getOrCreate()
    
    with pytest.raises(ValueError):
        builder = DisPyCanoeBuilder(
            spark=spark,
            drop_densest_as_needed=True,
            coalesce_densest_as_needed=True,
        )

def test_builder_feature_filter():
    """Test builder with feature filter function."""
    spark = SparkSession.builder.getOrCreate()
    
    def feature_filter(feature):
        return feature["properties"]["value"] > 10
    
    builder = DisPyCanoeBuilder(
        spark=spark,
        feature_filter=feature_filter,
    )
    
    assert builder.feature_filter is not None
    assert builder.feature_filter({"properties": {"value": 15}}) is True
    assert builder.feature_filter({"properties": {"value": 5}}) is False

def test_builder_output_format():
    """Test builder with different output formats."""
    spark = SparkSession.builder.getOrCreate()
    
    builder = DisPyCanoeBuilder(
        spark=spark,
        output_format="pmtiles",
    )
    
    assert builder.output_format == "pmtiles"
    
    with pytest.raises(ValueError):
        builder = DisPyCanoeBuilder(
            spark=spark,
            output_format="invalid",
        )

def test_builder_custom_spark():
    """Test builder with custom Spark session."""
    custom_spark = SparkSession.builder \
        .appName("TestSession") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    builder = DisPyCanoeBuilder(
        spark=custom_spark,
    )
    
    assert builder.spark == custom_spark

def test_load_data(spark, java_config, sample_geojson):
    """Test loading GeoJSON data into Spark DataFrame."""
    if not java_config["is_valid"]:
        pytest.skip("Skipping test that requires real Spark session")
    
    builder = DisPyCanoeBuilder(
        input_path=sample_geojson,
        output_path="test.pmtiles"
    )
    
    df = builder._load_data()
    rows = df.collect()
    
    assert len(rows) == 3
    assert all(row.geometry and row.properties for row in rows)

def test_density_reduction(spark, java_config, dense_geojson):
    """Test density reduction strategies."""
    if not java_config["is_valid"]:
        pytest.skip("Skipping test that requires real Spark session")
    
    builder = DisPyCanoeBuilder(
        input_path=dense_geojson,
        output_path="test.pmtiles",
        drop_densest_as_needed=True,
        feature_limit=20
    )
    
    features_df = builder._load_data()
    features_df = create_quadtree_partitioner(features_df)
    reduced_df = builder._reduce_density(features_df, [-180, -90, 180, 90])
    
    original_count = features_df.count()
    reduced_count = reduced_df.count()
    
    assert reduced_count < original_count
    assert reduced_count <= builder.feature_limit

def test_coalesce_features(spark, java_config, dense_geojson):
    """Test feature coalescing."""
    if not java_config["is_valid"]:
        pytest.skip("Skipping test that requires real Spark session")
    
    builder = DisPyCanoeBuilder(
        input_path=dense_geojson,
        output_path="test.pmtiles",
        coalesce_densest_as_needed=True,
        coalesce_fraction=0.3,
        min_points_to_coalesce=3
    )
    
    features_df = builder._load_data()
    features_df = create_quadtree_partitioner(features_df)
    coalesced_df = builder._reduce_density(features_df, [-180, -90, 180, 90])
    
    coalesced_features = coalesced_df.collect()
    has_coalesced = any(
        json.loads(row.properties).get("coalesced", False)
        for row in coalesced_features
    )
    assert has_coalesced

def test_full_pipeline(spark, java_config, sample_geojson, temp_dir):
    """Test the full PMTiles generation pipeline."""
    if not java_config["is_valid"]:
        pytest.skip("Skipping test that requires real Spark session")
    
    output_path = os.path.join(temp_dir, "output.pmtiles")
    
    builder = DisPyCanoeBuilder(
        input_path=sample_geojson,
        output_path=output_path,
        min_zoom=0,
        max_zoom=2
    )
    
    builder.build()
    
    assert os.path.exists(output_path)
    assert os.path.getsize(output_path) > 0

def test_quadtree_density_reduction(spark, java_config, dense_geojson):
    """Test density reduction using quad tree partitioning."""
    if not java_config["is_valid"]:
        pytest.skip("Skipping test that requires real Spark session")
    
    builder = DisPyCanoeBuilder(
        input_path=dense_geojson,
        output_path="test.pmtiles",
        drop_densest_as_needed=True,
        feature_limit=20
    )
    
    features_df = builder._load_data()
    features_df = create_quadtree_partitioner(features_df)
    
    assert "quad_cell" in features_df.columns
    
    reduced_df = builder._reduce_density(features_df, [-180, -90, 180, 90])
    assert reduced_df.count() <= builder.feature_limit 