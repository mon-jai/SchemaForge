"""
Command Line Interface for Schema Reader and Converter

Provides commands for scanning schemas and converting JSON files.
"""

import argparse
import logging
import sys
from pathlib import Path
from src.schema_reader import SchemaReader
from src.converter import Converter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def scan_schemas(args):
    """Scan JSON files and generate schema report."""
    logger.info("Starting schema scan...")
    
    try:
        reader = SchemaReader(
            data_dir=args.data_dir,
            max_sample_size=args.max_sample_size,
            sampling_strategy=args.sampling_strategy
        )
        
        schemas = reader.scan_directory()
        
        if not schemas:
            logger.warning("No schemas were generated. Check if JSON files exist in the data directory.")
            return 1
        
        logger.info(f"Successfully scanned {len(schemas)} file(s)")
        
        # Generate report
        report_path = reader.generate_report(args.output_report)
        logger.info(f"Schema report generated: {report_path}")
        
        return 0
    
    except FileNotFoundError as e:
        logger.error(f"Error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error during schema scan: {e}")
        return 1


def convert_files(args):
    """Convert JSON files to specified format."""
    logger.info(f"Starting conversion to {args.format}...")
    
    if args.format.lower() not in ['parquet', 'csv']:
        logger.error(f"Unsupported format: {args.format}. Supported formats: parquet, csv")
        return 1
    
    # Determine schema report path
    schema_report_path = args.schema_report
    if not schema_report_path:
        # Auto-detect: if markdown report path is provided, use corresponding JSON
        if args.schema_report_md:
            schema_report_path = Path(args.schema_report_md).with_suffix('.json')
        else:
            # Default: use default report path
            schema_report_path = "reports/schema_report.json"
    
    schema_report_path = str(Path(schema_report_path).resolve())
    
    # Check if schema report exists
    if not Path(schema_report_path).exists():
        logger.error(
            f"Schema report not found: {schema_report_path}\n"
            "Please run 'scan-schemas' command first to generate a schema report."
        )
        return 1
    
    try:
        converter = Converter(
            data_dir=args.data_dir,
            output_dir=args.output_dir,
            schema_report_path=schema_report_path
        )
        
        results = converter.convert_all(args.format.lower())
        
        if not results:
            logger.warning("No files were converted. Check if JSON files exist in the data directory.")
            return 1
        
        # Report results
        successful = sum(1 for success in results.values() if success)
        failed = len(results) - successful
        
        logger.info(f"Conversion complete: {successful} successful, {failed} failed")
        
        if failed > 0:
            logger.warning("Failed files:")
            for filename, success in results.items():
                if not success:
                    logger.warning(f"  - {filename}")
        
        return 0 if failed == 0 else 1
    
    except FileNotFoundError as e:
        logger.error(f"Error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error during conversion: {e}")
        return 1


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Schema Reader and Converter - JSON schema inference and format conversion tool"
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Scan schemas command
    scan_parser = subparsers.add_parser('scan-schemas', help='Scan JSON files and generate schema report')
    scan_parser.add_argument(
        '--data-dir',
        type=str,
        default='data',
        help='Directory containing JSON files (default: data)'
    )
    scan_parser.add_argument(
        '--output-report',
        type=str,
        default='reports/schema_report.md',
        help='Output path for schema report (default: reports/schema_report.md)'
    )
    scan_parser.add_argument(
        '--max-sample-size',
        type=int,
        default=None,
        help='Maximum number of records to sample per file (default: all records)'
    )
    scan_parser.add_argument(
        '--sampling-strategy',
        type=str,
        choices=['first', 'random'],
        default='first',
        help='Sampling strategy: first or random (default: first)'
    )
    
    # Convert command
    convert_parser = subparsers.add_parser('convert', help='Convert JSON files to Parquet or CSV')
    convert_parser.add_argument(
        '--format',
        type=str,
        required=True,
        choices=['parquet', 'csv'],
        help='Output format: parquet or csv'
    )
    convert_parser.add_argument(
        '--data-dir',
        type=str,
        default='data',
        help='Directory containing JSON files (default: data)'
    )
    convert_parser.add_argument(
        '--output-dir',
        type=str,
        default='output',
        help='Directory for output files (default: output)'
    )
    convert_parser.add_argument(
        '--schema-report',
        type=str,
        default=None,
        help='Path to schema report JSON file (default: reports/schema_report.json). '
             'Schema report must be generated first using scan-schemas command.'
    )
    convert_parser.add_argument(
        '--schema-report-md',
        type=str,
        default=None,
        help='Path to schema report Markdown file (alternative to --schema-report). '
             'The corresponding JSON file will be used automatically.'
    )
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    if args.command == 'scan-schemas':
        return scan_schemas(args)
    elif args.command == 'convert':
        return convert_files(args)
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())

