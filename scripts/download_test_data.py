#!/usr/bin/env python3
"""
Script to download test data files for dispycanoe.
"""
import pathlib
import requests

def download_file(url: str, output_path: pathlib.Path) -> None:
    """Download a file from a URL to the specified path."""
    if output_path.exists():
        print(f"File already exists at {output_path}")
        return
        
    print(f"Downloading {url} to {output_path}")
    response = requests.get(url)
    response.raise_for_status()
    
    # Create parent directories if they don't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write the content
    output_path.write_bytes(response.content)
    print(f"Successfully downloaded {len(response.content):,} bytes to {output_path}")

def main():
    """Main entry point."""
    # Define the project root as 2 levels up from this script
    project_root = pathlib.Path(__file__).parent.parent
    data_dir = project_root / "tests" / "data"
    
    print(f"Using data directory: {data_dir}")
    
    # Natural Earth populated places
    ne_url = "https://d2ad6b4ur7yvpq.cloudfront.net/naturalearth-3.3.0/ne_50m_populated_places.geojson"
    ne_output = data_dir / "populated_places.geojson"
    download_file(ne_url, ne_output)

if __name__ == "__main__":
    main() 