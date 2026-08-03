#!/usr/bin/env python3
"""Script to compile proto files using grpcio-tools."""

import os
import sys
import subprocess
from pathlib import Path

def generate_protos():
    try:
        import grpc_tools.protoc
    except ImportError:
        print("Error: grpcio-tools is not installed.", file=sys.stderr)
        print("Please install it using: pip install grpcio-tools", file=sys.stderr)
        sys.exit(1)

    project_root = Path(__file__).parent.parent
    proto_dir = project_root / "proto"
    output_dir = project_root / "src" / "grpc_service" / "generated"

    if not proto_dir.exists():
        print(f"Error: Proto directory {proto_dir} does not exist.", file=sys.stderr)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Ensure __init__.py exists in output dir
    (output_dir / "__init__.py").touch()

    proto_file = proto_dir / "cache.proto"
    
    if not proto_file.exists():
        print(f"Error: Proto file {proto_file} does not exist.", file=sys.stderr)
        sys.exit(1)

    print(f"Compiling {proto_file}...")
    
    command = [
        sys.executable,
        "-m", "grpc_tools.protoc",
        f"-I{proto_dir}",
        f"--python_out={output_dir}",
        f"--grpc_python_out={output_dir}",
        str(proto_file)
    ]
    
    result = subprocess.run(command, capture_output=True, text=True)
    
    if result.returncode != 0:
        print("Failed to compile proto files:", file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        sys.exit(result.returncode)
        
    print("Successfully compiled proto files to:", output_dir)

if __name__ == "__main__":
    generate_protos()
