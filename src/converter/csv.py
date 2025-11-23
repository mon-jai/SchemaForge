import logging
from pathlib import Path
from typing import Optional
from src.schema_reader import SchemaReader, FileSchema
from src.json_loader import load_json_file
from src.converter.utils import prepare_dataframe

logger = logging.getLogger(__name__)

def convert_to_csv(filepath: Path, output_dir: Path, schema_reader: SchemaReader, schema: Optional[FileSchema] = None) -> bool:
    """Convert a JSON file to CSV format."""
    logger.info(f"Converting {filepath.name} to CSV...")
    
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
        output_filename = filepath.stem + ".csv"
        output_path = output_dir / output_filename
        
        # Write to CSV
        df.to_csv(output_path, index=False, encoding='utf-8')
        
        logger.info(f"Successfully converted {filepath.name} to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to convert {filepath.name} to CSV: {e}")
        return False
