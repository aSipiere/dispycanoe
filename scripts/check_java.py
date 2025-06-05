#!/usr/bin/env python3
import os
import sys
import subprocess
from typing import Optional

def get_java_version() -> Optional[str]:
    """Get the Java version if available."""
    try:
        result = subprocess.run(['java', '-version'], 
                              capture_output=True, 
                              text=True, 
                              stderr=subprocess.STDOUT)
        return result.stdout
    except FileNotFoundError:
        return None

def check_java_home() -> Optional[str]:
    """Check if JAVA_HOME is set and valid."""
    java_home = os.environ.get('JAVA_HOME')
    if not java_home:
        return None
    
    java_exec = os.path.join(java_home, 'bin', 'java')
    return java_home if os.path.isfile(java_exec) else None

def main():
    """Check Java configuration and print results."""
    print("Checking Java configuration...")
    print("-" * 50)
    
    # Check Java version
    java_version = get_java_version()
    if java_version:
        print("✓ Java is installed:")
        print(java_version.strip())
    else:
        print("✗ Java is not installed or not in PATH")
        print("  Please install Java 11 or later")
    
    # Check JAVA_HOME
    java_home = check_java_home()
    if java_home:
        print("\n✓ JAVA_HOME is set correctly:")
        print(f"  {java_home}")
    else:
        print("\n✗ JAVA_HOME is not set or invalid")
        print("  Please set JAVA_HOME to your Java installation directory")
    
    # Print recommendations if there are issues
    if not java_version or not java_home:
        print("\nRecommendations:")
        print("1. Using conda (recommended):")
        print("   conda install -c conda-forge openjdk=11")
        print("\n2. Manual installation:")
        print("   Ubuntu/Debian:")
        print("   sudo apt-get install openjdk-11-jdk")
        print("   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64")
        print("\n   CentOS/RHEL:")
        print("   sudo yum install java-11-openjdk-devel")
        print("   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk")
        sys.exit(1)
    
    print("\n✓ Java configuration looks good!")
    sys.exit(0)

if __name__ == '__main__':
    main() 