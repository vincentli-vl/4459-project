import grpc
import time
import threading
import mapreduce_pb2
import mapreduce_pb2_grpc
import logging
from concurrent import futures
import subprocess
import sys
import os

logging.basicConfig(level=logging.INFO)

def start_master(port):
    """Start the master process"""
    process = subprocess.Popen([sys.executable, 'master.py', str(port)])
    time.sleep(2)  # Give master time to start
    return process

def start_worker(worker_id, worker_port, master_port):
    """Start a worker process"""
    process = subprocess.Popen([
        sys.executable, 'worker.py', 
        str(worker_id), 
        str(worker_port)
    ])
    time.sleep(1)  # Give worker time to start
    return process

def submit_job(master_port, job_name, input_file):
    """Submit a job to the master"""
    channel = grpc.insecure_channel(f'localhost:{master_port}')
    stub = mapreduce_pb2_grpc.MapReduceStub(channel)
    
    request = mapreduce_pb2.JobRequest(
        job_name=job_name,
        input_file=input_file
    )
    
    try:
        response = stub.SubmitJob(request)
        return response
    except grpc.RpcError as e:
        logging.error(f"Failed to submit job: {e}")
        return None

def run_test():
    master_port = 50051
    worker_ports = [50052, 50053, 50054]  # Start 3 workers
    
    # Create test input file
    from create_test_file import create_test_file
    create_test_file('test_input.txt')
    
    try:
        # Start master
        logging.info("Starting master...")
        master_process = start_master(master_port)
        
        # Start workers
        worker_processes = []
        for i, port in enumerate(worker_ports):
            logging.info(f"Starting worker {i+1}...")
            worker_process = start_worker(f"worker_{i+1}", port, master_port)
            worker_processes.append(worker_process)
        
        # Submit job
        logging.info("Submitting job...")
        response = submit_job(master_port, "word_count", "test_input.txt")
        
        if response and response.success:
            logging.info(f"Job submitted successfully. Job ID: {response.job_id}")
            
            # Wait for job completion
            time.sleep(10)
            
            # Check output files
            output_dir = f"/tmp/mapreduce/{response.job_id}"
            if os.path.exists(output_dir):
                logging.info("Job completed. Checking results...")
            else:
                logging.error("Output directory not found")
        else:
            logging.error("Failed to submit job")
            
    except Exception as e:
        logging.error(f"Test failed: {e}")
        
    finally:
        # Cleanup
        logging.info("Cleaning up...")
        master_process.terminate()
        for worker_process in worker_processes:
            worker_process.terminate()

        logging.info(f"Job ID: {response.job_id}")
        logging.info(f"Verify results by inputting \"python verify_results.py {response.job_id}\"")
        

if __name__ == '__main__':
    run_test() 