import logging
import json
from pathlib import Path
from typing import Dict, Any, Optional

from src.schema_reader import SchemaReader
from src.benchmark.utils import get_file_size, format_size, measure_resources
from src.benchmark.reporting import generate_schema_markdown_report

logger = logging.getLogger(__name__)

def run_schema_benchmark(data_dir: Path, result_dir: Path, max_sample_size: Optional[int] = None) -> Dict[str, Any]:
    """
    Run comprehensive schema inference benchmarks.
    
    Returns:
        Dict containing benchmark results.
    """
    logger.info("Starting schema inference benchmark...")
    
    schema_result_dir = result_dir / "schema"
    schema_result_dir.mkdir(parents=True, exist_ok=True)
    
    reader = SchemaReader(data_dir=str(data_dir), max_sample_size=max_sample_size)
    
    # Get list of JSON files
    json_files = list(data_dir.glob("*.json"))
    
    results = {
        "summary": {
            "total_files": len(json_files),
            "max_sample_size": max_sample_size
        },
        "per_file": {},
        "total": {}
    }
    
    # Benchmark each file individually
    for json_file in json_files:
        logger.info(f"Benchmarking schema inference for {json_file.name}...")
        
        file_size = get_file_size(json_file)
        
        metrics = measure_resources(reader.infer_schema, json_file)
        
        schema = metrics["result"]
        
        results["per_file"][json_file.name] = {
            "input_file_size": file_size,
            "input_file_size_formatted": format_size(file_size),
            "record_count": schema.record_count if schema else 0,
            "field_count": len(schema.fields) if schema else 0,
            "execution_time_seconds": round(metrics["execution_time"], 3),
            "peak_memory_mb": round(metrics["peak_memory_mb"], 2),
            "memory_increase_mb": round(metrics["memory_increase_mb"], 2),
            "cpu_percent": round(metrics["cpu_percent"], 2),
            "throughput_records_per_second": round(schema.record_count / metrics["execution_time"], 2) if schema and metrics["execution_time"] > 0 else 0
        }
    
    # Overall benchmark (scan all)
    logger.info("Running full directory scan benchmark...")
    overall_metrics = measure_resources(reader.scan_directory)
    
    results["total"] = {
        "execution_time_seconds": round(overall_metrics["execution_time"], 3),
        "peak_memory_mb": round(overall_metrics["peak_memory_mb"], 2),
        "memory_increase_mb": round(overall_metrics["memory_increase_mb"], 2),
        "cpu_percent": round(overall_metrics["cpu_percent"], 2)
    }
    
    # Save results
    output_file = schema_result_dir / "schema_benchmark.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"Schema benchmark results saved to {output_file}")
    
    # Generate markdown report
    generate_schema_markdown_report(results, schema_result_dir)
    
    return results
