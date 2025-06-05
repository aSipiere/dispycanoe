from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import pyproj
from pmtiles import writer
from pmtiles.tile import TileType, Compression, zxy_to_tileid
from .tile import Tile
from .geojson import load_geojson
from .spatial import create_quadtree_partitioner, get_quad_cell_bounds
from .density import reduce_density

class DisPyCanoeBuilder:
    """A distributed Python implementation of Mapbox's tippecanoe using PySpark."""

    def __init__(
        self,
        spark=None,
        max_zoom=14,
        min_zoom=0,
        tile_size=512,
        buffer_size=64,
        max_points_per_tile=100000,
        min_points_per_tile=1000,
        max_points_total=None,
        feature_filter=None,
        output_dir="tiles",
        output_format="pmtiles",
        drop_densest_as_needed: bool = False,
        coalesce_densest_as_needed: bool = False,
        drop_fraction: float = 0.75,
        coalesce_fraction: float = 0.6,
        min_points_to_coalesce: int = 4
    ):
        """Initialize the DisPyCanoeBuilder.
        
        Args:
            spark: Spark session
            max_zoom: Maximum zoom level
            min_zoom: Minimum zoom level
            tile_size: Size of tiles in pixels
            buffer_size: Buffer size for tiles in pixels
            max_points_per_tile: Maximum number of features per tile
            min_points_per_tile: Minimum number of features per tile
            max_points_total: Maximum total number of features
            feature_filter: Filter function for features
            output_dir: Output directory for tiles
            output_format: Output format for tiles
            drop_densest_as_needed: Whether to drop dense features when needed
            coalesce_densest_as_needed: Whether to coalesce dense features when needed
            drop_fraction: Fraction of features to drop when reducing density
            coalesce_fraction: Fraction of features to coalesce when reducing density
            min_points_to_coalesce: Minimum number of points needed to attempt coalescing
        """
        if min_zoom > max_zoom:
            raise ValueError("min_zoom must be less than or equal to max_zoom")
        
        if drop_densest_as_needed and coalesce_densest_as_needed:
            raise ValueError("Cannot enable both drop_densest_as_needed and coalesce_densest_as_needed")
        
        self.max_zoom = max_zoom
        self.min_zoom = min_zoom
        self.tile_size = tile_size
        self.buffer_size = buffer_size
        self.max_points_per_tile = max_points_per_tile
        self.min_points_per_tile = min_points_per_tile
        self.max_points_total = max_points_total
        self.feature_filter = feature_filter
        self.output_dir = output_dir
        self.output_format = output_format
        self.drop_densest_as_needed = drop_densest_as_needed
        self.coalesce_densest_as_needed = coalesce_densest_as_needed
        self.drop_fraction = drop_fraction
        self.coalesce_fraction = coalesce_fraction
        self.min_points_to_coalesce = min_points_to_coalesce
        
        self.spark = (
            spark
            if spark
            else SparkSession.builder.master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .appName("DisPyCanoe")
        )
        
        # Initialize projections
        self.web_mercator = pyproj.CRS('EPSG:3857')
        self.wgs84 = pyproj.CRS('EPSG:4326')
        self.project = pyproj.Transformer.from_crs(
            self.wgs84,
            self.web_mercator,
            always_xy=True
        ).transform
    
    def _load_data(self) -> DataFrame:
        """Load GeoJSON data into a distributed Spark DataFrame."""
        return load_geojson(self.spark, self.input_path)
    
    def _get_tile_coords(self, quad_cell: str, zoom: int) -> tuple[int, int]:
        """Convert quad cell code to tile coordinates."""
        bounds = get_quad_cell_bounds(quad_cell)
        x = int((bounds[0] + 180.0) / 360.0 * (1 << zoom))
        y = int((1.0 - (bounds[1] + 90.0) / 180.0) * (1 << zoom))
        return x, y
    
    def _reduce_density(self, features_df: DataFrame, bounds: list[float]) -> DataFrame:
        """Reduce feature density using configured strategy."""
        return reduce_density(
            features_df,
            bounds,
            self.max_points_per_tile,
            self.drop_densest_as_needed,
            self.coalesce_densest_as_needed,
            self.drop_fraction,
            self.coalesce_fraction,
            self.min_points_to_coalesce
        )
    
    def _process_tiles_batch(self, features_df: DataFrame) -> None:
        """Process a batch of tiles and write to PMTiles."""
        # Create PMTiles writer
        pmtiles = writer.Writer(
            self.output_path,
            compression=Compression.GZIP
        )
        
        # Process each zoom level
        for zoom in range(self.min_zoom, self.max_zoom + 1):
            # For each zoom level, start with the original features
            zoom_df = features_df
            
            # Add quad tree partitioning with level based on zoom
            # Higher zoom = more detailed quadtree
            max_level = min(zoom + 4, 10)  # Cap at level 10 to avoid too many cells
            zoom_df = create_quadtree_partitioner(zoom_df, max_level=max_level)
            
            # Reduce density if needed, with feature limit scaled by zoom
            # Lower zooms = fewer features allowed
            zoom_feature_limit = max(
                self.max_points_per_tile // (2 ** (self.max_zoom - zoom)),
                100  # Minimum features to keep
            )
            zoom_df = self._reduce_density(zoom_df, [-180, -90, 180, 90])
            
            # Group features by quad cell
            grouped = zoom_df.groupBy("quad_cell").agg(
                F.collect_list(
                    F.struct("geometry", "properties")
                ).alias("features")
            ).collect()
            
            # Create and write tiles
            for row in grouped:
                x, y = self._get_tile_coords(row.quad_cell, zoom)
                tile = Tile(zoom, x, y, row.features)
                geojson = tile.to_geojson()
                
                # Write tile to PMTiles
                tile_id = zxy_to_tileid(zoom, x, y)
                pmtiles.write_tile(tile_id, geojson)
        
        # Finalize PMTiles file
        pmtiles.finish()
    
    def build(self):
        """Build PMTiles from input GeoJSON."""
        features_df = self._load_data()
        self._process_tiles_batch(features_df)