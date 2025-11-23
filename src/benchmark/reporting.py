import logging
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)

def generate_schema_markdown_report(results: Dict[str, Any], output_dir: Path):
    """Generate markdown report for schema benchmark."""
    output_file = output_dir / "schema_benchmark_report.md"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# Schema Inference Benchmark Report\n\n")
        f.write(f"**Total Files**: {results['summary']['total_files']}\n\n")
        f.write(f"**Max Sample Size**: {results['summary']['max_sample_size'] or 'All records'}\n\n")
        
        f.write("## Overall Performance\n\n")
        f.write(f"- **Execution Time**: {results['total']['execution_time_seconds']}s\n")
        f.write(f"- **Peak Memory**: {results['total']['peak_memory_mb']} MB\n")
        f.write(f"- **CPU Usage**: {results['total']['cpu_percent']}%\n\n")
        
        f.write("## Per-File Results\n\n")
        f.write("| File | Size | Records | Fields | Time (s) | Memory (MB) | Throughput (rec/s) |\n")
        f.write("|------|------|---------|--------|----------|-------------|--------------------|\n")
        
        for filename, data in results["per_file"].items():
            f.write(f"| {filename} | {data['input_file_size_formatted']} | {data['record_count']} | {data['field_count']} | {data['execution_time_seconds']} | {data['peak_memory_mb']} | {data['throughput_records_per_second']} |\n")
    
    logger.info(f"Schema benchmark markdown report saved to {output_file}")

def generate_conversion_markdown_report(results: Dict[str, Any], output_dir: Path):
    """Generate markdown report for conversion benchmark."""
    output_file = output_dir / "conversion_benchmark_report.md"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# Conversion Benchmark Report\n\n")
        f.write(f"**Total Files**: {results['summary']['total_files']}\n\n")
        f.write(f"**Formats Tested**: {', '.join(results['summary']['formats_tested'])}\n\n")
        
        # Format Comparison Summary
        f.write("## 📊 Format Comparison Summary\n\n")
        f.write("| Format | Time (s) | Memory (MB) | CPU (%) | Output Size | Compression (%) |\n")
        f.write("|--------|----------|-------------|---------|-------------|------------------|\n")
        
        for format_type, format_data in results["per_format"].items():
            time_val = format_data['total']['execution_time_seconds']
            mem_val = format_data['total']['peak_memory_mb']
            cpu_val = format_data['total']['cpu_percent']
            size_val = format_data['total']['total_output_size_formatted']
            compress_val = format_data['total']['overall_compression_ratio_percent']
            f.write(f"| **{format_type.upper()}** | {time_val} | {mem_val} | {cpu_val} | {size_val} | {compress_val}% |\n")
        
        f.write("\n### 🏆 Winner Analysis\n\n")
        
        # Find winners
        best_time_format = min(results["per_format"].items(), key=lambda x: x[1]['total']['execution_time_seconds'])
        best_compression_format = max(results["per_format"].items(), key=lambda x: x[1]['total']['overall_compression_ratio_percent'])
        best_size_format = min(results["per_format"].items(), key=lambda x: x[1]['total']['total_output_size'])
        
        f.write(f"- **⚡ Fastest Conversion**: {best_time_format[0].upper()} ({best_time_format[1]['total']['execution_time_seconds']}s)\n")
        f.write(f"- **🗜️ Best Compression**: {best_compression_format[0].upper()} ({best_compression_format[1]['total']['overall_compression_ratio_percent']}%)\n")
        f.write(f"- **📦 Smallest Output**: {best_size_format[0].upper()} ({best_size_format[1]['total']['total_output_size_formatted']})\n\n")
        
        f.write("---\n\n")
        
        # Detailed per-format results
        for format_type, format_data in results["per_format"].items():
            f.write(f"## {format_type.upper()} Format\n\n")
            f.write("### Overall Performance\n\n")
            f.write(f"- **Execution Time**: {format_data['total']['execution_time_seconds']}s\n")
            f.write(f"- **Peak Memory**: {format_data['total']['peak_memory_mb']} MB\n")
            f.write(f"- **CPU Usage**: {format_data['total']['cpu_percent']}%\n")
            f.write(f"- **Total Input Size**: {format_data['total']['total_input_size_formatted']}\n")
            f.write(f"- **Total Output Size**: {format_data['total']['total_output_size_formatted']}\n")
            f.write(f"- **Compression Ratio**: {format_data['total']['overall_compression_ratio_percent']}%\n\n")
            
            f.write("### Per-File Results\n\n")
            f.write("| File | Input Size | Output Size | Compression | Size Reduction |\n")
            f.write("|------|------------|-------------|-------------|----------------|\n")
            
            for filename, file_data in format_data["per_file"].items():
                f.write(f"| {filename} | {file_data['input_size_formatted']} | {file_data['output_size_formatted']} | {file_data['compression_ratio_percent']}% | {file_data['size_reduction']} |\n")
            
            f.write("\n")
    
    logger.info(f"Conversion benchmark markdown report saved to {output_file}")
