import time
import psutil
import os
from pathlib import Path
from typing import Dict, Any

def get_file_size(filepath: Path) -> int:
    """Get file size in bytes."""
    return filepath.stat().st_size if filepath.exists() else 0

def format_size(size_bytes: int) -> str:
    """Format size in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

def measure_resources(func, *args, **kwargs) -> Dict[str, Any]:
    """
    Measure execution time, CPU usage, and memory for a function.
    
    Returns:
        Dict with metrics: execution_time, peak_memory_mb, avg_cpu_percent
    """
    process = psutil.Process(os.getpid())
    
    # Reset CPU percent measurement
    process.cpu_percent(interval=None)
    
    # Record initial memory
    mem_before = process.memory_info().rss / (1024 * 1024)  # MB
    
    # Start timing
    start_time = time.time()
    
    # Execute function
    result = func(*args, **kwargs)
    
    # End timing
    execution_time = time.time() - start_time
    
    # Record peak memory
    mem_after = process.memory_info().rss / (1024 * 1024)  # MB
    peak_memory = mem_after
    
    # Get CPU usage (since start of measurement)
    cpu_percent = process.cpu_percent(interval=None)
    
    return {
        "result": result,
        "execution_time": execution_time,
        "peak_memory_mb": peak_memory,
        "memory_increase_mb": mem_after - mem_before,
        "cpu_percent": cpu_percent
    }
