from typing import Dict, Any, Union, Set, Optional

class SchemaField:
    """Represents a single field in a schema with its properties."""
    
    def __init__(self, name: str, field_type: Union[str, Set[str]], nullable: bool = False,
                 example_value: Any = None, is_nested: bool = False, nested_fields: Optional[Dict] = None,
                 distinct_values: Optional[Set[Any]] = None, min_value: Optional[Any] = None,
                 max_value: Optional[Any] = None, min_length: Optional[int] = None,
                 max_length: Optional[int] = None, avg_length: Optional[float] = None):
        self.name = name
        self.field_type = field_type
        self.nullable = nullable
        self.example_value = example_value
        self.is_nested = is_nested
        self.nested_fields = nested_fields or {}
        self.distinct_values = distinct_values or set()
        self.min_value = min_value
        self.max_value = max_value
        self.min_length = min_length
        self.max_length = max_length
        self.avg_length = avg_length
    
    def __repr__(self):
        type_str = self.field_type if isinstance(self.field_type, str) else f"mixed({', '.join(sorted(self.field_type))})"
        return f"SchemaField(name={self.name}, type={type_str}, nullable={self.nullable})"


class FileSchema:
    """Represents the schema of a single JSON file."""
    
    def __init__(self, filename: str, record_count: int, fields: Dict[str, SchemaField]):
        self.filename = filename
        self.record_count = record_count
        self.fields = fields
    
    def __repr__(self):
        return f"FileSchema(filename={self.filename}, records={self.record_count}, fields={len(self.fields)})"
