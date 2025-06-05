import pytest
import os
import tempfile
import json
from pyspark.sql import SparkSession
import subprocess
import re
from typing import Optional, Tuple

def parse_java_version(version_str: str) -> Optional[int]:
    """Parse Java version string to get major version number."""
    if not version_str:
        return None
    
    # Try to match version patterns
    patterns = [
        r'version "([0-9]+)',  # Matches: version "17.0.1"
        r'([0-9]+)\.',         # Matches: 17.0.1
        r'([0-9]+)-',          # Matches: 17-ea
    ]
    
    for pattern in patterns:
        match = re.search(pattern, version_str)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                continue
    return None

def check_java_environment() -> Tuple[bool, Optional[str], Optional[str]]:
    """Check if Java environment is properly configured."""
    # Check JAVA_HOME
    java_home = os.environ.get("JAVA_HOME")
    if not java_home or not os.path.exists(java_home):
        return False, None, None
    
    # Check Java version
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            env={"JAVA_HOME": java_home}
        )
        version_str = result.stderr  # Java -version outputs to stderr
        version = parse_java_version(version_str)
        
        if version and version >= 17:  # PySpark requires Java 17+
            return True, version_str, java_home
        return False, version_str, java_home
    except Exception:
        return False, None, java_home

@pytest.fixture(scope="session")
def java_config():
    """Check if Java is available and return configuration."""
    is_valid, version_str, java_home = check_java_environment()
    return {
        "is_valid": is_valid,
        "version": version_str,
        "java_home": java_home
    }

@pytest.fixture(scope="session")
def spark(java_config):
    """Create a Spark session for testing."""
    if not java_config["is_valid"]:
        pytest.skip("Skipping test that requires real Spark session")
    
    # Set JAVA_HOME for Spark
    os.environ["JAVA_HOME"] = java_config["java_home"]
    
    # Create Spark session with test-optimized configuration
    spark = SparkSession.builder \
        .appName("DisPyCanoeTest") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    
    # Initialize context
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    # Clean up
    spark.stop()

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir

@pytest.fixture
def sample_geojson(temp_dir):
    """Create a sample GeoJSON file for testing."""
    filepath = os.path.join(temp_dir, "sample.geojson")
    
    features = [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [0, 0]
            },
            "properties": {"id": "point_1"}
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [1, 1]
            },
            "properties": {"id": "point_2"}
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [-1, -1]
            },
            "properties": {"id": "point_3"}
        }
    ]
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    with open(filepath, 'w') as f:
        json.dump(geojson, f)
    
    yield filepath

@pytest.fixture
def dense_geojson(temp_dir):
    """Create a GeoJSON file with dense point clusters for testing coalescing."""
    filepath = os.path.join(temp_dir, "dense.geojson")
    
    features = []
    
    # Dense cluster of points
    for i in range(50):
        x = 45 + (i % 5) * 0.01
        y = 45 + (i // 5) * 0.01
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [x, y]
            },
            "properties": {
                "id": f"point_{i}"
            }
        })
    
    # Add a few scattered points
    for i in range(5):
        x = -90 + i * 30
        y = -45 + i * 20
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [x, y]
            },
            "properties": {
                "id": f"scattered_{i}"
            }
        })
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    with open(filepath, 'w') as f:
        json.dump(geojson, f)
    
    return filepath

@pytest.fixture
def invalid_geojson(temp_dir):
    """Create a GeoJSON file with invalid features."""
    filepath = os.path.join(temp_dir, "invalid.geojson")
    
    features = [
        {
            "type": "Feature",
            "geometry": {
                "type": "Invalid",
                "coordinates": [0, 0]
            },
            "properties": {}
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": []  # Empty coordinates
            },
            "properties": {}
        }
    ]
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    with open(filepath, 'w') as f:
        json.dump(geojson, f)
    
    yield filepath

@pytest.fixture
def mixed_geojson(temp_dir):
    """Create a GeoJSON file with mixed valid/invalid features."""
    filepath = os.path.join(temp_dir, "mixed.geojson")
    
    # 8 valid features, 2 invalid (20% error rate)
    features = []
    
    # Valid features
    for i in range(8):
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [i, i]
            },
            "properties": {"id": f"valid_{i}"}
        })
    
    # Invalid features
    features.extend([
        {
            "type": "Feature",
            "geometry": {
                "type": "Invalid",
                "coordinates": [0, 0]
            },
            "properties": {}
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": []
            },
            "properties": {}
        }
    ])
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    with open(filepath, 'w') as f:
        json.dump(geojson, f)
    
    yield filepath

@pytest.fixture
def standard_geojson(temp_dir):
    """Create a standard GeoJSON file with multiple points."""
    filepath = os.path.join(temp_dir, "standard.geojson")
    
    features = [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [0, 0]
            },
            "properties": {"id": "Point_1"}
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [1, 1]
            },
            "properties": {"id": "Point_2"}
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [-1, -1]
            },
            "properties": {"id": "Point_3"}
        }
    ]
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    with open(filepath, 'w') as f:
        json.dump(geojson, f)
    
    yield filepath 