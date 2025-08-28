#!/usr/bin/env python3
"""
Script to run Flink Python jobs
Usage: python run_job.py <job_name>
Available jobs: word_count, customer_analytics, customer_debezium
"""

import sys
import os
import importlib.util


def main():
    if len(sys.argv) != 2:
        print("Usage: python run_job.py <job_name>")
        print("Available jobs: word_count, customer_analytics, customer_debezium")
        sys.exit(1)
    
    job_name = sys.argv[1].lower()
    
    # Map job names to file names
    job_mapping = {
        'word_count': 'word_count_job.py',
        'customer_analytics': 'customer_analytics_job.py', 
        'customer_debezium': 'customer_debezium_job.py'
    }
    
    if job_name not in job_mapping:
        print(f"Unknown job: {job_name}")
        print("Available jobs:", ", ".join(job_mapping.keys()))
        sys.exit(1)
    
    # Get the path to the job file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    job_file = os.path.join(script_dir, 'src', 'jobs', job_mapping[job_name])
    
    if not os.path.exists(job_file):
        print(f"Job file not found: {job_file}")
        sys.exit(1)
    
    print(f"Running job: {job_name}")
    print(f"Job file: {job_file}")
    
    # Load and execute the job module
    spec = importlib.util.spec_from_file_location("job_module", job_file)
    job_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(job_module)
    
    # Run the main function
    if hasattr(job_module, 'main'):
        job_module.main()
    else:
        print("No main function found in job file")
        sys.exit(1)


if __name__ == "__main__":
    main()


