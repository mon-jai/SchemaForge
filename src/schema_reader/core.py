import logging
from pathlib import Path
from typing import Dict, List, Any, Optional

from src.schema_reader.types import FileSchema, SchemaField
from src.schema_reader.utils import (
    looks_like_timestamp, looks_like_url, looks_like_email, looks_like_uuid,
    looks_like_ip_address, looks_like_numeric_string, looks_like_json_string,
    flatten_dict, extract_columns_from_metadata, convert_array_row_to_object
)
from src.schema_reader.inference import infer_type, analyze_field, infer_schema, scan_directory
from src.schema_reader.reporting import generate_report, save_schemas_to_json, load_schemas_from_json

logger = logging.getLogger(__name__)

class SchemaReader:
    """Main class for reading and inferring schemas from JSON files."""
    
    def __init__(self, data_dir: str = "data", max_sample_size: Optional[int] = None,
                 sampling_strategy: str = "first"):
        """
        Initialize the SchemaReader.
        
        Args:
            data_dir: Directory containing JSON files
            max_sample_size: Maximum number of records to sample per file (None = all)
            sampling_strategy: 'first' or 'random' sampling strategy
        """
        self.data_dir = Path(data_dir)
        self.max_sample_size = max_sample_size
        self.sampling_strategy = sampling_strategy
        self.schemas: Dict[str, FileSchema] = {}
    
    # Expose helper methods for backward compatibility
    def _infer_type(self, value: Any) -> str:
        return infer_type(value)
    
    def _looks_like_timestamp(self, value: str) -> bool:
        return looks_like_timestamp(value)
    
    def _looks_like_url(self, value: str) -> bool:
        return looks_like_url(value)
    
    def _looks_like_email(self, value: str) -> bool:
        return looks_like_email(value)
    
    def _looks_like_uuid(self, value: str) -> bool:
        return looks_like_uuid(value)
    
    def _looks_like_ip_address(self, value: str) -> bool:
        return looks_like_ip_address(value)
    
    def _looks_like_numeric_string(self, value: str) -> bool:
        return looks_like_numeric_string(value)
    
    def _looks_like_json_string(self, value: str) -> bool:
        return looks_like_json_string(value)
    
    def _flatten_dict(self, d: Dict, parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
        return flatten_dict(d, parent_key, sep)
    
    def _analyze_field(self, values: List[Any], field_name: str) -> SchemaField:
        return analyze_field(values, field_name)
    
    def _extract_columns_from_metadata(self, metadata: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        return extract_columns_from_metadata(metadata)
    
    def _convert_array_row_to_object(self, row: List[Any], columns: List[Dict[str, Any]]) -> Dict[str, Any]:
        return convert_array_row_to_object(row, columns)
    
    def _sample_records(self, records: List[Dict[str, Any]], sample_size: Optional[int] = None) -> List[Dict[str, Any]]:
        # This was a helper method in the original class, but logic is now embedded in inference.py
        # Re-implementing for backward compatibility if needed, using the logic from inference.py
        from src.schema_reader.inference import sample_records
        size = sample_size if sample_size is not None else self.max_sample_size
        return sample_records(records, size, self.sampling_strategy)
    
    def infer_schema(self, filepath: Path) -> Optional[FileSchema]:
        """Infer schema for a single JSON file."""
        return infer_schema(filepath, self.max_sample_size, self.sampling_strategy)
    
    def scan_directory(self) -> Dict[str, FileSchema]:
        """Scan the data directory and infer schemas for all JSON files."""
        self.schemas = scan_directory(self.data_dir, self.max_sample_size, self.sampling_strategy)
        return self.schemas
    
    def generate_report(self, output_path: str = "reports/schema_report.md") -> str:
        """Generate a human-readable schema report."""
        return generate_report(self.schemas, output_path)
    
    def save_schemas_to_json(self, output_path: str = "reports/schema_report.json") -> str:
        """Save schemas to JSON format for machine reading."""
        return save_schemas_to_json(self.schemas, output_path)
    
    @classmethod
    def load_schemas_from_json(cls, json_path: str) -> Dict[str, FileSchema]:
        """Load schemas from a JSON file."""
        return load_schemas_from_json(json_path)
