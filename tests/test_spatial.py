import pytest
import json
from shapely.geometry import Point, mapping
from dispycanoe.spatial import QuadCell, assign_quad_cell, create_quadtree_partitioner, get_quad_cell_bounds

def test_quad_cell_initialization():
    """Test QuadCell initialization and properties."""
    cell = QuadCell([-180, -90, 180, 90], 0)
    assert cell.bounds == [-180, -90, 180, 90]
    assert cell.level == 0
    assert cell.code == ""

def test_quad_cell_subdivision():
    """Test QuadCell subdivision."""
    cell = QuadCell([-180, -90, 180, 90], 0)
    children = cell.subdivide()
    
    assert len(children) == 4
    
    # Check SW quadrant
    assert children[0].bounds == [-180, -90, 0, 0]
    assert children[0].level == 1
    assert children[0].code == "0"
    
    # Check SE quadrant
    assert children[1].bounds == [0, -90, 180, 0]
    assert children[1].level == 1
    assert children[1].code == "1"
    
    # Check NW quadrant
    assert children[2].bounds == [-180, 0, 0, 90]
    assert children[2].level == 1
    assert children[2].code == "2"
    
    # Check NE quadrant
    assert children[3].bounds == [0, 0, 180, 90]
    assert children[3].level == 1
    assert children[3].code == "3"

def test_assign_quad_cell_point():
    """Test quad cell assignment for points."""
    point = Point(90, 45)
    geom_str = json.dumps(mapping(point))
    
    code = assign_quad_cell(geom_str, max_level=1)
    assert code == "3"  # NE quadrant

def test_get_quad_cell_bounds():
    """Test bounds calculation from Morton code."""
    # Test root cell
    assert get_quad_cell_bounds("") == [-180, -90, 180, 90]
    
    # Test NE quadrant
    ne_bounds = get_quad_cell_bounds("3")
    assert ne_bounds == [0, 0, 180, 90]
    
    # Test deeper level
    deep_bounds = get_quad_cell_bounds("301")  # NE -> SW -> SE
    expected = [45.0, 0.0, 90.0, 22.5]
    assert deep_bounds == expected

def test_create_quadtree_partitioner(spark):
    """Test quadtree partitioning of features."""
    features = [
        (json.dumps(mapping(Point(90, 45))), "{}"),    # NE
        (json.dumps(mapping(Point(-90, -45))), "{}"),  # SW
        (json.dumps(mapping(Point(45, -45))), "{}")    # SE
    ]
    
    schema = "geometry STRING, properties STRING"
    df = spark.createDataFrame(features, schema)
    
    assert df.count() == 3
    
    result_df = create_quadtree_partitioner(df)
    partitioned = result_df.collect()
    
    assert len(partitioned) == 3
    assert all("quad_cell" in row.asDict() for row in partitioned) 