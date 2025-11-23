import logging
import pyarrow as pa
import pyarrow.orc as orc
from pathlib import Path
from typing import Optional
from src.schema_reader import SchemaReader, FileSchema
from src.json_loader import load_json_file
from src.converter.utils import prepare_dataframe

logger = logging.getLogger(__name__)

def convert_to_orc(filepath: Path, output_dir: Path, schema_reader: SchemaReader, schema: Optional[FileSchema] = None) -> bool:
    """Convert a JSON file to ORC format."""
    logger.info(f"Converting {filepath.name} to ORC...")
    
    try:
        # Load schema if not provided
        if schema is None:
            schema = schema_reader.infer_schema(filepath)
            if schema is None:
                logger.error(f"Failed to infer schema for {filepath.name}")
                return False
        
        # Load JSON data
        records = load_json_file(filepath, stream=False)
        
        if not records:
            logger.warning(f"No records to convert in {filepath.name}")
            return False
        
        # Prepare DataFrame
        df = prepare_dataframe(records, schema)
        
        if df.empty:
            logger.warning(f"Empty DataFrame created for {filepath.name}")
            return False
        
        # Generate output filename
        output_filename = filepath.stem + ".orc"
        output_path = output_dir / output_filename
        
        # Convert to PyArrow table and handle null columns
        table = pa.Table.from_pandas(df)
        
        # Filter out columns with null type (PyArrow can't write them to ORC)
        schema_fields = []
        valid_columns = []
        for i, field in enumerate(table.schema):
            if field.type != pa.null():
                schema_fields.append(field)
                valid_columns.append(table.column(i))
            else:
                logger.warning(f"Skipping null-type column '{field.name}' in {filepath.name} for ORC")
        
        if not valid_columns:
            logger.error(f"No valid columns for ORC conversion in {filepath.name}")
            return False
        
        # Create new table with only valid columns
        filtered_table = pa.Table.from_arrays(valid_columns, schema=pa.schema(schema_fields))
        orc.write_table(filtered_table, output_path)
        
        logger.info(f"Successfully converted {filepath.name} to {output_path}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to convert {filepath.name} to ORC: {e}")
        return False
