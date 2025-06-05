# DisPyCanoe

A distributed Python implementation of Mapbox's tippecanoe using PySpark.

## Requirements

### Java Requirements
DisPyCanoe uses PySpark 3.5.0 which requires Java 17 or later. If you see errors about "UnsupportedClassVersionError" or "class file version 61.0", you need to upgrade your Java installation:

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# macOS with Homebrew
brew install openjdk@17
export JAVA_HOME=/usr/local/opt/openjdk@17

# Verify installation
java -version  # Should show version 17 or later
```

Add to your `~/.bashrc` or `~/.zshrc`:
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64  # Ubuntu/Debian
# or
export JAVA_HOME=/usr/local/opt/openjdk@17  # macOS
export PATH=$JAVA_HOME/bin:$PATH
```

### Python Requirements
- Python 3.8 or later
- Dependencies listed in `requirements.txt`

## Installation

1. Create a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt  # for development
```

## Usage

```python
from dispycanoe import TileForgeBuilder

builder = TileForgeBuilder(
    input_path="path/to/your/geojson",
    output_path="output.pmtiles",
    min_zoom=0,
    max_zoom=14
)

builder.build()
```

## Development

### Running Tests
```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/

# Run tests with coverage
pytest tests/ --cov=dispycanoe
```

### Common Issues

#### Java Version Error
If you see an error like:
```
java.lang.UnsupportedClassVersionError: org/apache/spark/launcher/Main has been compiled by a more recent version of the Java Runtime (class file version 61.0)
```
This means you need to upgrade to Java 17 or later. See the Java Requirements section above.

#### JAVA_HOME Not Set
If you see "JAVA_HOME is not set" errors, make sure to set it in your shell profile:
```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
``` 