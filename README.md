# Schema Reader and Converter

A robust Python tool for inferring schemas from JSON files and converting them to Parquet and CSV formats. This project is designed for data engineering workflows where you need to understand and transform JSON data efficiently.

## Features

- **Schema Inference**: Automatically detects field names, data types, nested structures, and nullable fields
- **Multiple JSON Formats**: Supports both JSON array format and newline-delimited JSON (NDJSON)
- **Format Conversion**: Converts JSON to Parquet and CSV with proper type handling
- **Nested Structure Handling**: Flattens nested objects with dot notation (e.g., `user.address.city`)
- **Configurable Sampling**: Supports sampling strategies for large files
- **Human-Readable Reports**: Generates Markdown schema reports with detailed field information
- **Robust Error Handling**: Gracefully handles malformed files and continues processing

## Project Structure

```
project_root/
  data/                # Input JSON files (place your JSON files here)
  output/              # Converted outputs (Parquet, CSV)
  reports/             # Schema reports
  src/
    __init__.py
    schema_reader.py   # Schema inference module
    converter.py       # Format conversion module
    cli.py             # Command line interface
  tests/
    test_schema_reader.py
    test_converter.py
  README.md
  requirements.txt
```

## Installation

1. **Clone or download this project**

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

   Required packages:
   - `pandas` (>=2.0.0) - Data manipulation
   - `pyarrow` (>=12.0.0) - Parquet file support
   - `pytest` (>=7.0.0) - For running tests

## Usage

### 1. Scan Schemas

Scan all JSON files in the `data/` directory and generate a schema report:

```bash
python -m src.cli scan-schemas
```

This will:
- Scan all `.json` files in the `data/` directory
- Infer schemas for each file
- Generate a Markdown report at `reports/schema_report.md`

**Options:**
- `--data-dir`: Specify custom data directory (default: `data`)
- `--output-report`: Specify custom report path (default: `reports/schema_report.md`)
- `--max-sample-size`: Limit number of records to analyze per file (default: all records)
- `--sampling-strategy`: Choose `first` or `random` sampling (default: `first`)

**Example:**
```bash
python -m src.cli scan-schemas --data-dir my_data --max-sample-size 1000
```

### 2. Convert to Parquet

Convert all JSON files to Parquet format:

```bash
python -m src.cli convert --format parquet
```

**Options:**
- `--format`: Output format (`parquet` or `csv`)
- `--data-dir`: Input directory (default: `data`)
- `--output-dir`: Output directory (default: `output`)

**Example:**
```bash
python -m src.cli convert --format parquet --data-dir my_data --output-dir my_output
```

### 3. Convert to CSV

Convert all JSON files to CSV format:

```bash
python -m src.cli convert --format csv
```

**Example:**
```bash
python -m src.cli convert --format csv --output-dir csv_output
```

## Supported JSON Formats

The tool supports two JSON formats:

1. **JSON Array Format**:
   ```json
   [
     {"id": 1, "name": "Alice"},
     {"id": 2, "name": "Bob"}
   ]
   ```

2. **Newline-Delimited JSON (NDJSON)**:
   ```json
   {"id": 1, "name": "Alice"}
   {"id": 2, "name": "Bob"}
   ```

## Schema Inference

The schema reader automatically detects:

- **Basic Types**: string, integer, float, boolean, null
- **Complex Types**: array, object (nested structures)
- **Special Types**: timestamp (detected from common date/time patterns)
- **Nullable Fields**: Fields that contain `null` values
- **Mixed Types**: Fields with inconsistent types across records
- **Nested Structures**: Objects within objects, arrays of objects

## Nested Structure Handling

Nested objects are flattened using dot notation:

**Input:**
```json
{
  "user": {
    "name": "Alice",
    "address": {
      "city": "New York"
    }
  }
}
```

**Output Columns:**
- `user.name`
- `user.address.city`

Arrays of objects are converted to JSON strings in a single column for CSV/Parquet compatibility.

## Example Workflow

1. **Place JSON files in the `data/` directory**:
   ```bash
   cp my_data/*.json data/
   ```

2. **Scan schemas**:
   ```bash
   python -m src.cli scan-schemas
   ```

3. **Review the schema report**:
   ```bash
   cat reports/schema_report.md
   ```

4. **Convert to Parquet**:
   ```bash
   python -m src.cli convert --format parquet
   ```

5. **Convert to CSV**:
   ```bash
   python -m src.cli convert --format csv
   ```

## Testing

Run the test suite:

```bash
pytest tests/
```

Or run specific test files:

```bash
pytest tests/test_schema_reader.py
pytest tests/test_converter.py
```

## Error Handling

The tool is designed to be robust:

- **Empty Directory**: Warns if no JSON files are found
- **Malformed JSON**: Logs errors and continues processing other files
- **Missing Fields**: Handles missing fields gracefully (filled with `None`)
- **Type Coercion Failures**: Logs warnings but continues processing
- **Per-File Processing**: One bad file doesn't stop the entire batch

## Known Limitations and Assumptions

1. **Memory**: Large files are loaded entirely into memory. For very large files, use the `--max-sample-size` option for schema inference.

2. **Array Handling**: Arrays of objects are stored as JSON strings in CSV/Parquet output. This is a design choice to maintain compatibility with flat file formats.

3. **Type Coercion**: Type coercion is best-effort. Values that cannot be coerced are preserved as-is with a warning.

4. **Timestamp Detection**: Timestamp detection uses pattern matching. Complex or non-standard date formats may not be detected.

5. **Encoding**: Files are assumed to be UTF-8 encoded.

6. **File Format Detection**: The tool attempts to auto-detect JSON array vs NDJSON format, but may fail on edge cases.

## Extending the Project

The codebase is modular and designed for extension:

- **New Output Formats**: Add conversion methods to `converter.py`
- **Custom Type Inference**: Extend `_infer_type()` in `schema_reader.py`
- **Different Sampling Strategies**: Implement new strategies in `SchemaReader`
- **Schema Validation**: Add validation logic using the inferred schemas

## License

This project is provided as-is for educational and data engineering purposes.

## Contributing

Feel free to extend this project with additional features such as:
- Support for more output formats (Avro, ORC, etc.)
- Schema validation against inferred schemas
- Incremental schema updates
- Database export capabilities
- More sophisticated nested structure handling

