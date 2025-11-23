import json
import re
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

def looks_like_timestamp(value: str) -> bool:
    """Check if a string looks like a timestamp."""
    if not isinstance(value, str) or len(value) < 10:
        return False
    
    # Common timestamp patterns
    patterns = [
        r'^\d{4}-\d{2}-\d{2}',  # ISO date (YYYY-MM-DD)
        r'^\d{4}-\d{2}-\d{2}T',  # ISO datetime (YYYY-MM-DDTHH:MM:SS)
        r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}',  # Date time with space
        r'^\d{4}/\d{2}/\d{2}',  # Date with slashes
        r'^\d{2}/\d{2}/\d{4}',  # US date format
        r'^\d{10}$',  # Unix timestamp (seconds)
        r'^\d{13}$',  # Unix timestamp (milliseconds)
        r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}',  # ISO with timezone
        r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z',  # ISO with Z
    ]
    
    for pattern in patterns:
        if re.match(pattern, value):
            return True
    return False

def looks_like_url(value: str) -> bool:
    """Check if a string looks like a URL."""
    if not isinstance(value, str):
        return False
    
    # Basic URL pattern
    url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
    return bool(re.match(url_pattern, value, re.IGNORECASE))

def looks_like_email(value: str) -> bool:
    """Check if a string looks like an email address."""
    if not isinstance(value, str):
        return False
    
    # Basic email pattern
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(email_pattern, value))

def looks_like_uuid(value: str) -> bool:
    """Check if a string looks like a UUID."""
    if not isinstance(value, str):
        return False
    
    # UUID pattern (with or without hyphens)
    uuid_pattern = r'^[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}$'
    return bool(re.match(uuid_pattern, value, re.IGNORECASE))

def looks_like_ip_address(value: str) -> bool:
    """Check if a string looks like an IP address."""
    if not isinstance(value, str):
        return False
    
    # IPv4 pattern
    ipv4_pattern = r'^(\d{1,3}\.){3}\d{1,3}$'
    if re.match(ipv4_pattern, value):
        # Validate each octet is 0-255
        try:
            parts = value.split('.')
            if all(0 <= int(part) <= 255 for part in parts):
                return True
        except ValueError:
            pass
    
    # IPv6 pattern (simplified)
    ipv6_pattern = r'^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$|^::1$|^::$'
    return bool(re.match(ipv6_pattern, value))

def looks_like_numeric_string(value: str) -> bool:
    """Check if a string contains only numeric characters (could be parsed as number)."""
    if not isinstance(value, str) or not value.strip():
        return False
    
    # Matches integers, floats, scientific notation, with optional leading/trailing whitespace
    numeric_pattern = r'^\s*[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?\s*$'
    return bool(re.match(numeric_pattern, value))

def looks_like_json_string(value: str) -> bool:
    """Check if a string looks like embedded JSON."""
    if not isinstance(value, str) or len(value) < 2:
        return False
    
    value = value.strip()
    # Check if it starts and ends with JSON-like delimiters
    if (value.startswith('{') and value.endswith('}')) or \
       (value.startswith('[') and value.endswith(']')):
        # Try to parse it
        try:
            json.loads(value)
            return True
        except (json.JSONDecodeError, ValueError):
            pass
    
    return False

def flatten_dict(d: Dict, parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
    """Flatten a nested dictionary."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
            # Array of objects - store as array type but note the structure
            items.append((new_key, v))
        else:
            items.append((new_key, v))
    return dict(items)

def extract_columns_from_metadata(metadata: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """Extract column definitions from metadata (e.g., Socrata/OpenData format)."""
    # Try various metadata paths
    metadata_paths = [
        ['meta', 'view', 'columns'],
        ['view', 'columns'],
        ['columns'],
        ['schema', 'fields'],
        ['fields'],
        ['header']
    ]
    
    for path in metadata_paths:
        current = metadata
        found = True
        for key in path:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                found = False
                break
        
        if found and isinstance(current, list) and len(current) > 0:
            # Check if it looks like column definitions
            if isinstance(current[0], dict) and ('name' in current[0] or 'fieldName' in current[0]):
                logger.info(f"Found column definitions in metadata path: {' -> '.join(path)}")
                return current
    
    return None

def convert_array_row_to_object(row: List[Any], columns: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Convert an array-based row to an object using column definitions."""
    record = {}
    
    for col_def in columns:
        # Get column name - try multiple field names
        col_name = None
        for name_field in ['fieldName', 'name', 'id', 'key']:
            if name_field in col_def:
                col_name = str(col_def[name_field])
                break
        
        if col_name is None:
            # Fallback to position-based name
            position = col_def.get('position', len(record))
            col_name = f"column_{position}"
        
        # Skip hidden/meta columns if flagged
        if col_def.get('flags') and 'hidden' in col_def.get('flags', []):
            continue
        
        # Skip meta_data type columns by default (can be overridden)
        if col_def.get('dataTypeName') == 'meta_data' and 'hidden' in col_def.get('flags', []):
            continue
        
        # Get value from row using position
        # Position in Socrata/OpenData format is 0-indexed and matches array index
        # But we need to account for hidden columns that might be skipped
        position = col_def.get('position', -1)
        
        # If position is valid and within row bounds
        if position >= 0 and position < len(row):
            value = row[position]
        else:
            # Fallback: try to use index in columns list
            col_idx = columns.index(col_def) if col_def in columns else -1
            if col_idx >= 0 and col_idx < len(row):
                value = row[col_idx]
            else:
                continue  # Skip if we can't map this column
        
        # Clean column name (remove : prefix if present)
        clean_name = col_name.lstrip(':')
        record[clean_name] = value
    
    return record
