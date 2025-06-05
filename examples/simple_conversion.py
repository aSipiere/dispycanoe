from tippy import TippyBuilder
import os

def main():
    # Create output directory if it doesn't exist
    os.makedirs("output", exist_ok=True)

    # Initialize the builder with density reduction options
    builder = TippyBuilder(
        input_path="data/sample.geojson",
        output_path="output/result.pmtiles",
        min_zoom=0,
        max_zoom=14,
        tile_size=512,
        buffer=64,
        feature_limit=200000,
        # Tune these parameters based on your cluster
        partition_size=100,  # Number of tiles per partition
        # Enable density reduction options
        drop_densest_as_needed=True,      # Similar to tippecanoe's --drop-densest-as-needed
        coalesce_densest_as_needed=True,  # Similar to tippecanoe's --coalesce-densest-as-needed
        drop_fraction=0.75,               # Drop 75% of densest features when needed
        coalesce_fraction=0.6,            # Coalesce top 60% densest features when needed
        min_points_to_coalesce=4          # Minimum points needed for coalescing
    )
    
    # Generate PMTiles in parallel
    builder.build()

if __name__ == "__main__":
    main() 