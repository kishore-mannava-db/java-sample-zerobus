#!/usr/bin/env python3

import argparse
import sys
import re
from typing import Optional, List, Dict, Tuple
import requests
import json
from urllib.parse import urljoin, quote


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    description = """
Generate proto2 file from Unity Catalog table schema.
This script fetches table schema from Unity Catalog and generates a corresponding proto2 definition file.
    """

    epilog = """
Examples:
    # Generate proto file for a Unity Catalog table
    python generate_proto.py \\
        --uc-endpoint "https://your-workspace.cloud.databricks.com" \\
        --uc-token "dapi1234567890" \\
        --table "catalog.schema.table_name" \\
        --proto-msg "TableMessage" \\
        --output "output.proto"

Type mappings:
    Delta            -> Proto2
    INT              -> int32
    STRING           -> string
    FLOAT            -> float
    LONG             -> int64
    SHORT            -> int32
    DOUBLE           -> double
    BOOLEAN          -> bool
    BINARY           -> bytes
    DATE             -> int32
    TIMESTAMP        -> int64
    ARRAY<type>      -> repeated type
    MAP<key_type, value_type> -> map<key_type, value_type>
    """

    parser = argparse.ArgumentParser(
        description=description, epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--uc-endpoint",
        type=str,
        required=True,
        help="Unity Catalog endpoint URL (e.g., https://your-workspace.cloud.databricks.com)",
    )

    parser.add_argument(
        "--uc-token",
        type=str,
        required=True,
        help="Unity Catalog authentication token (e.g., dapi123...)",
    )

    parser.add_argument(
        "--table",
        type=str,
        required=True,
        help="Full table name in format: catalog.schema.table_name",
    )

    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output path for the generated proto file (e.g., output.proto)",
    )

    parser.add_argument(
        "--proto-msg",
        type=str,
        required=False,
        help="Name of the protobuf message (defaults to table_name)",
    )

    return parser.parse_args()


def fetch_table_info(endpoint: str, token: str, table: str) -> Dict[str, str]:
    """
    Fetch table information from Unity Catalog.

    Args:
        endpoint: Base URL of the Unity Catalog endpoint
        token: Authentication token
        table: Table identifier

    Returns:
        Dictionary containing the table information

    Raises:
        requests.exceptions.RequestException: If the HTTP request fails
    """
    encoded_table = quote(table)
    url = urljoin(endpoint, f"/api/2.1/unity-catalog/tables/{encoded_table}")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response.json()


def extract_columns(table_info: dict) -> List[Dict[str, str]]:
    """
    Extract column information from the table schema.

    Args:
        table_info: Raw table information from Unity Catalog

    Returns:
        List of dictionaries containing column name and type information

    Raises:
        KeyError: If the expected schema structure is not found
    """
    try:
        columns = table_info["columns"]
        return [
            {"name": col["name"], "type_text": col["type_text"], "nullable": col["nullable"]}
            for col in columns
        ]
    except KeyError as e:
        raise KeyError(f"Failed to extract column information: missing key {e}")


def parse_array_type(column_type: str) -> Optional[str]:
    """
    Parse array type and extract the element type.

    Args:
        column_type: The Unity Catalog column type (e.g., "array<string>")

    Returns:
        Element type if it's an array, None otherwise
    """
    match = re.match(r"^ARRAY<(.+)>$", column_type.upper())
    if match:
        return match.group(1).strip()
    return None


def parse_map_type(column_type: str) -> Optional[Tuple[str, str]]:
    """
    Parse map type and extract key and value types.

    Args:
        column_type: The Unity Catalog column type (e.g., "map<string,int>")

    Returns:
        Tuple of (key_type, value_type) if it's a map, None otherwise
    """
    match = re.match(r"^MAP<(.+),(.+)>$", column_type.upper())
    if match:
        key_type = match.group(1).strip()
        value_type = match.group(2).strip()
        return (key_type, value_type)
    return None


def get_proto_field_info(column_type: str, nullable: bool) -> Tuple[str, str]:
    """
    Map Unity Catalog column types to proto2 field information.

    Args:
        column_type: The Unity Catalog column type
        nullable: Whether the column is nullable

    Returns:
        Tuple of (field_modifier, proto_type) where field_modifier is 'optional', 'repeated' or empty string in case of a map.

    Raises:
        ValueError: If the column type is not supported
    """
    type_mapping = {
        "SMALLINT": "int32",
        "INT": "int32",
        "STRING": "string",
        "FLOAT": "float",
        "BIGINT": "int64",
        "LONG": "int64",
        "SHORT": "int32",
        "DOUBLE": "double",
        "BOOLEAN": "bool",
        "BINARY": "bytes",
        "DATE": "int32",
        "TIMESTAMP": "int64",
    }

    proto_type = type_mapping.get(column_type.upper())
    if proto_type is not None:
        return ("optional" if nullable else "required", proto_type)

    if column_type.upper().startswith("VARCHAR"):
        return ("optional" if nullable else "required", "string")

    element_type = parse_array_type(column_type)
    if element_type is not None:
        element_proto_type = type_mapping.get(element_type.upper())
        if element_proto_type is None:
            raise ValueError(f"Unsupported array element type: {element_type}")
        return ("repeated", element_proto_type)

    map_types = parse_map_type(column_type)
    if map_types is not None:
        key_type, value_type = map_types
        key_proto_type = type_mapping.get(key_type.upper())
        if key_proto_type is None:
            raise ValueError(f"Unsupported map key type: {key_type}")
        value_proto_type = type_mapping.get(value_type.upper())
        if value_proto_type is None:
            raise ValueError(f"Unsupported map value type: {value_type}")
        return ("", f"map<{key_proto_type}, {value_proto_type}>")

    raise ValueError(f"Unsupported column type: {column_type}")


def generate_proto_file(message_name: str, columns: List[Dict[str, str]], output_path: str) -> None:
    """
    Generate a proto2 file from the column information.

    Args:
        message_name: Name of the protobuf message
        columns: List of column information dictionaries
        output_path: Path where to write the proto file
    """
    proto_content = [
        'syntax = "proto2";',
        "",
        f"message {message_name} {{",
    ]

    # Add fields
    for idx, col in enumerate(columns, start=1):
        field_modifier, proto_type = get_proto_field_info(col["type_text"], col["nullable"])
        field_name = col["name"]
        if field_modifier == "":
            proto_content.append(f"    {proto_type} {field_name} = {idx};")
        else:
            proto_content.append(f"    {field_modifier} {proto_type} {field_name} = {idx};")

    proto_content.append("}")
    proto_content.append("")  # Add final newline

    # Write to file
    with open(output_path, "w") as f:
        f.write("\n".join(proto_content))


def main() -> Optional[int]:
    """Main function to process the arguments and execute the script logic."""
    args = parse_args()

    try:
        # Fetch table information from Unity Catalog
        table_info = fetch_table_info(args.uc_endpoint, args.uc_token, args.table)

        # Extract column information
        columns = extract_columns(table_info)

        # If proto_msg is not provided, use the table name
        message_name = args.proto_msg if args.proto_msg else args.table.split(".")[-1]

        # Generate proto file
        generate_proto_file(message_name, columns, args.output)

        print(f"Successfully generated proto file at: {args.output}")
        return 0

    except requests.exceptions.RequestException as e:
        print(f"Error making request to Unity Catalog: {e}", file=sys.stderr)
        return 1
    except KeyError as e:
        print(f"Error processing table schema: {e}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"Error mapping column type: {e}", file=sys.stderr)
        return 1
    except IOError as e:
        print(f"Error writing proto file: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
