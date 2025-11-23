import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

from src.schema_reader.types import FileSchema, SchemaField

logger = logging.getLogger(__name__)

def save_schemas_to_json(schemas: Dict[str, FileSchema], output_path: str = "reports/schema_report.json") -> str:
    """Save schemas to JSON format for machine reading."""
    if not schemas:
        logger.warning("No schemas available to save.")
        return ""
    
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    schemas_dict = {}
    for filename, schema in schemas.items():
        schema_data = {
            "filename": schema.filename,
            "record_count": schema.record_count,
            "fields": {}
        }
        
        for field_name, field in schema.fields.items():
            # Convert field_type to serializable format
            if isinstance(field.field_type, set):
                field_type_serialized = list(field.field_type)
            else:
                field_type_serialized = field.field_type
            
            # Convert distinct_values to serializable format
            distinct_values_serialized = None
            if field.distinct_values:
                distinct_values_serialized = [str(v) if not isinstance(v, (str, int, float, bool)) else v 
                                               for v in list(field.distinct_values)[:100]]  # Limit to 100
            
            schema_data["fields"][field_name] = {
                "name": field.name,
                "field_type": field_type_serialized,
                "nullable": field.nullable,
                "example_value": str(field.example_value) if field.example_value is not None else None,
                "is_nested": field.is_nested,
                "nested_fields": field.nested_fields if field.nested_fields else {},
                "distinct_values": distinct_values_serialized,
                "min_value": field.min_value,
                "max_value": field.max_value,
                "min_length": field.min_length,
                "max_length": field.max_length,
                "avg_length": field.avg_length
            }
        
        schemas_dict[filename] = schema_data
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(schemas_dict, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Schemas saved to JSON: {output_file}")
    return str(output_file)

def generate_report(schemas: Dict[str, FileSchema], output_path: str = "reports/schema_report.md") -> str:
    """Generate a human-readable schema report."""
    if not schemas:
        logger.warning("No schemas available to generate report.")
        return ""
    
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    lines = []
    lines.append("# JSON Schema Report")
    lines.append("")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    for filename, schema in schemas.items():
        lines.append(f"## File: {filename}")
        lines.append("")
        lines.append(f"- **Records Scanned:** {schema.record_count}")
        lines.append(f"- **Fields Detected:** {len(schema.fields)}")
        lines.append("")
        lines.append("### Field Details")
        lines.append("")
        lines.append("| Field Name | Type | Nullable | Example Value | Statistics | Notes |")
        lines.append("|------------|------|----------|---------------|------------|-------|")
        
        for field_name in sorted(schema.fields.keys()):
            field = schema.fields[field_name]
            
            # Format type
            if isinstance(field.field_type, set):
                type_str = f"mixed({', '.join(sorted(field.field_type))})"
            else:
                type_str = field.field_type
            
            # Format example value
            example_str = str(field.example_value)
            if len(example_str) > 50:
                example_str = example_str[:47] + "..."
            example_str = example_str.replace("|", "\\|")  # Escape pipe for markdown
            
            # Format statistics
            stats = []
            if field.min_value is not None and field.max_value is not None:
                stats.append(f"min: {field.min_value}, max: {field.max_value}")
            if field.min_length is not None and field.max_length is not None:
                if field.avg_length is not None:
                    stats.append(f"len: {field.min_length}-{field.max_length} (avg: {field.avg_length:.1f})")
                else:
                    stats.append(f"len: {field.min_length}-{field.max_length}")
            if field.distinct_values and len(field.distinct_values) <= 10:
                # Show enum values if small set
                enum_vals = sorted([str(v) for v in list(field.distinct_values)[:10]])
                if len(enum_vals) <= 5:
                    stats.append(f"enum: {', '.join(enum_vals)}")
                else:
                    stats.append(f"enum: {len(field.distinct_values)} values")
            stats_str = "; ".join(stats) if stats else "-"
            stats_str = stats_str.replace("|", "\\|")  # Escape pipe for markdown
            
            # Notes
            notes = []
            if field.nullable:
                notes.append("nullable")
            if field.is_nested:
                notes.append("nested")
            if isinstance(field.field_type, set):
                notes.append("mixed types")
            if field.distinct_values and len(field.distinct_values) <= 20:
                notes.append("enum-like")
            notes_str = ", ".join(notes) if notes else "-"
            
            lines.append(f"| `{field_name}` | {type_str} | {'Yes' if field.nullable else 'No'} | `{example_str}` | {stats_str} | {notes_str} |")
        
        lines.append("")
        lines.append("---")
        lines.append("")
    
    report_content = "\n".join(lines)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    logger.info(f"Schema report written to {output_file}")
    
    # Also save schemas in JSON format for machine reading
    json_path = output_file.with_suffix('.json')
    save_schemas_to_json(schemas, str(json_path))
    
    return str(output_file)

def load_schemas_from_json(json_path: str) -> Dict[str, FileSchema]:
    """Load schemas from a JSON file."""
    json_file = Path(json_path)
    
    if not json_file.exists():
        raise FileNotFoundError(f"Schema report JSON not found: {json_path}")
    
    with open(json_file, 'r', encoding='utf-8') as f:
        schemas_dict = json.load(f)
    
    schemas = {}
    for filename, schema_data in schemas_dict.items():
        fields = {}
        
        for field_name, field_data in schema_data["fields"].items():
            # Convert field_type back from serialized format
            field_type = field_data["field_type"]
            if isinstance(field_type, list):
                field_type = set(field_type)
            
            # Convert distinct_values back from serialized format
            distinct_values = None
            if field_data.get("distinct_values"):
                distinct_values = set(field_data["distinct_values"])
            
            field = SchemaField(
                name=field_data["name"],
                field_type=field_type,
                nullable=field_data["nullable"],
                example_value=field_data.get("example_value"),
                is_nested=field_data.get("is_nested", False),
                nested_fields=field_data.get("nested_fields", {}),
                distinct_values=distinct_values,
                min_value=field_data.get("min_value"),
                max_value=field_data.get("max_value"),
                min_length=field_data.get("min_length"),
                max_length=field_data.get("max_length"),
                avg_length=field_data.get("avg_length")
            )
            fields[field_name] = field
        
        schema = FileSchema(
            filename=schema_data["filename"],
            record_count=schema_data["record_count"],
            fields=fields
        )
        schemas[filename] = schema
    
    logger.info(f"Loaded {len(schemas)} schema(s) from {json_path}")
    return schemas
