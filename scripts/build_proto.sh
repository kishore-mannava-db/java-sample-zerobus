#!/bin/bash
set -e

usage() {
  echo "Usage: $0 --uc-endpoint <url> --uc-token <token> --table <catalog.schema.table> --output-file-name <proto_file> [--proto-msg <message_name>]"
  exit 1
}

ORIGINAL_DIR="$(pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PROTO_MSG="Record"

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --uc-endpoint) UC_ENDPOINT="$2"; shift ;;
        --uc-token) UC_TOKEN="$2"; shift ;;
        --table) TABLE="$2"; shift ;;
        --output-file-name) FILE_NAME="$2"; shift ;;
        --proto-msg) PROTO_MSG="$2"; shift ;;
        *) echo "Unknown parameter: $1"; usage ;;
    esac
    shift
done

if [[ -z "$UC_ENDPOINT" || -z "$UC_TOKEN" || -z "$TABLE" || -z "$FILE_NAME" ]]; then
    echo "Missing required arguments."
    usage
fi

PROTO_NAME="${FILE_NAME%.proto}"
PROTO_FILE="$ORIGINAL_DIR/${FILE_NAME}"

if [[ ! -d "$SCRIPT_DIR/venv" ]]; then
  echo "Creating Python virtual environment..."
  python3 -m venv "$SCRIPT_DIR/venv"
fi

# Activate venv
source "$SCRIPT_DIR/venv/bin/activate"

# Install dependencies
pip install --quiet requests
echo "Running Python script to generate proto file..."

# Run python script to generate .proto file in current directory
python3 "$SCRIPT_DIR/generate_proto.py" \
  --uc-endpoint "$UC_ENDPOINT" \
  --uc-token "$UC_TOKEN" \
  --table "$TABLE" \
  --proto-msg "$PROTO_MSG" \
  --output "$PROTO_FILE"

deactivate
rm -rf "$SCRIPT_DIR/venv"

echo "Generating Java files from proto..."
# Use protoc to generate Java files in the current directory
protoc --proto_path="$ORIGINAL_DIR" --java_out="$ORIGINAL_DIR" "$PROTO_FILE"

echo "Done. Generated files:"
echo "- Proto file: $PROTO_FILE"
echo "- Java files: $ORIGINAL_DIR"