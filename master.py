import grpc
import uuid
import time
import threading
from concurrent import futures
import mapreduce_pb2
import mapreduce_pb2_grpc
import logging
from collections import defaultdict
from typing import Dict, List, Set
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class Worker:
    def __init__(self, id: str, address: str):
        self.id = id
        self.address = address
        self.status = "IDLE"
        self.current_task = None
        self.last_heartbeat = time.time()
        
    def get_stub(self):
        """Create a new channel and stub for each request"""
        channel = grpc.insecure_channel(self.address)
        return mapreduce_pb2_grpc.WorkerStub(channel)

class MapReduceMaster(mapreduce_pb2_grpc.MapReduceServicer):
    def __init__(self):
        self.workers: Dict[str, Worker] = {}
        self.active_jobs: Dict[str, dict] = {}
        self.completed_jobs: Set[str] = set()
        
        # Start worker monitoring thread
        self.monitor_thread = threading.Thread(target=self._monitor_workers)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

    def SubmitJob(self, request, context):
        """Handle new job submissions from clients"""
        try:
            job_id = str(uuid.uuid4())
            logging.info(f"Received new job request. Job ID: {job_id}")

            # Validate input file
            if not os.path.exists(request.input_file):
                return mapreduce_pb2.JobResponse(
                    success=False,
                    message=f"Input file {request.input_file} not found",
                    job_id=job_id
                )

            # Initialize job metadata
            self.active_jobs[job_id] = {
                'name': request.job_name,
                'input_file': request.input_file,
                'status': 'PENDING',
                'map_tasks': [],
                'reduce_tasks': [],
                'intermediate_results': defaultdict(list)
            }

            # Start job processing in a separate thread
            threading.Thread(target=self._process_job, args=(job_id,)).start()

            return mapreduce_pb2.JobResponse(
                success=True,
                message="Job submitted successfully",
                job_id=job_id
            )

        except Exception as e:
            logging.error(f"Error submitting job: {str(e)}")
            return mapreduce_pb2.JobResponse(
                success=False,
                message=f"Error submitting job: {str(e)}",
                job_id=""
            )

    def SendHeartbeat(self, request, context):
        """Handle worker heartbeats"""
        worker_id = request.worker_id
        
        # Register worker if it's new
        if worker_id not in self.workers:
            # Use the address sent by the worker
            worker_address = request.address
            self.register_worker(worker_id, worker_address)
            logging.info(f"New worker registered via heartbeat: {worker_id} at {worker_address}")
        
        # Update worker status
        worker = self.workers[worker_id]
        worker.status = request.status
        worker.current_task = request.current_task
        worker.last_heartbeat = time.time()
        
        return mapreduce_pb2.HeartbeatResponse(
            acknowledged=True,
            message="Heartbeat received"
        )

    def _process_job(self, job_id: str):
        """Process a MapReduce job"""
        try:
            job = self.active_jobs[job_id]
            job['status'] = 'MAPPING'

            # Read and split input file
            chunks = self._split_input_file(job['input_file'])
            
            # Assign map tasks
            map_results = self._assign_map_tasks(job_id, chunks)
            if not map_results:
                job['status'] = 'FAILED'
                return

            job['status'] = 'REDUCING'
            
            # Assign reduce tasks
            success = self._assign_reduce_tasks(job_id, map_results)
            if not success:
                job['status'] = 'FAILED'
                return

            # Job completed successfully
            job['status'] = 'COMPLETED'
            self.completed_jobs.add(job_id)
            del self.active_jobs[job_id]

        except Exception as e:
            logging.error(f"Error processing job {job_id}: {str(e)}")
            self.active_jobs[job_id]['status'] = 'FAILED'

    def _split_input_file(self, input_file: str, chunk_size=1024*1024) -> List[str]:
        """Split input file into chunks"""
        chunks = []
        with open(input_file, 'r') as f:
            current_chunk = []
            current_size = 0
            
            for line in f:
                line_size = len(line.encode('utf-8'))
                if current_size + line_size > chunk_size and current_chunk:
                    chunks.append(''.join(current_chunk))
                    current_chunk = [line]
                    current_size = line_size
                else:
                    current_chunk.append(line)
                    current_size += line_size
                    
            if current_chunk:
                chunks.append(''.join(current_chunk))
                
        return chunks

    def _assign_map_tasks(self, job_id: str, chunks: List[str]) -> List[mapreduce_pb2.KeyValuePair]:
        """Assign map tasks to available workers"""
        all_results = []
        
        logging.info(f"Starting map tasks for job {job_id} with {len(chunks)} chunks")
        logging.info(f"Available workers: {len(self.workers)}")
        
        for i, chunk in enumerate(chunks):
            task_id = f"{job_id}_map_{i}"
            assigned = False
            attempts = 0
            max_attempts = 3
            
            while not assigned and attempts < max_attempts:
                # Find available workers
                available_workers = [w for w in self.workers.values() 
                                   if w.status == "IDLE" and 
                                   time.time() - w.last_heartbeat < 10]  # Only consider recently active workers
                
                logging.info(f"Found {len(available_workers)} available workers")
                
                if not available_workers:
                    logging.warning("No available workers, waiting...")
                    time.sleep(2)
                    attempts += 1
                    continue

                worker = available_workers[0]
                logging.info(f"Assigning task {task_id} to worker {worker.id}")
                
                try:
                    task = mapreduce_pb2.MapTask(
                        task_id=task_id,
                        data_chunk=chunk,
                        output_location=f"/tmp/mapreduce/{job_id}/map_{i}"
                    )
                    
                    # Get a fresh stub for the request
                    stub = worker.get_stub()
                    response = stub.AssignMapTask(task)
                    
                    if response.success:
                        logging.info(f"Map task {task_id} completed successfully with {len(response.result)} results")
                        all_results.extend(response.result)
                        assigned = True
                    else:
                        logging.error(f"Map task {task_id} failed: {response.message}")
                        attempts += 1
                        
                except Exception as e:
                    logging.error(f"Error assigning map task to worker: {str(e)}")
                    attempts += 1
                    time.sleep(1)
        
        logging.info(f"All map tasks completed. Total results: {len(all_results)}")
        return all_results

    def _assign_reduce_tasks(self, job_id: str, map_results: List[mapreduce_pb2.KeyValuePair]) -> bool:
        """Assign reduce tasks to available workers"""
        try:
            # Group mapped data by key
            grouped_data = defaultdict(list)
            for pair in map_results:
                grouped_data[pair.key].append(pair)
            
            logging.info(f"Grouped {len(map_results)} map results into {len(grouped_data)} keys")
            
            # Create reduce tasks
            success = True
            for key, values in grouped_data.items():
                task_id = f"{job_id}_reduce_{key}"
                assigned = False
                attempts = 0
                max_attempts = 3
                
                while not assigned and attempts < max_attempts:
                    available_workers = [w for w in self.workers.values() 
                                       if w.status == "IDLE" and 
                                       time.time() - w.last_heartbeat < 10]
                    
                    if not available_workers:
                        logging.warning(f"No available workers for reduce task {task_id}")
                        time.sleep(2)
                        attempts += 1
                        continue

                    worker = available_workers[0]
                    try:
                        task = mapreduce_pb2.ReduceTask(
                            task_id=task_id,
                            mapped_data=values,
                            output_location=f"/tmp/mapreduce/{job_id}/reduce_{key}"
                        )
                        
                        logging.info(f"Assigning reduce task for key '{key}' with {len(values)} values to worker {worker.id}")
                        stub = worker.get_stub()
                        response = stub.AssignReduceTask(task)
                        
                        if response.success:
                            logging.info(f"Reduce task {task_id} completed successfully")
                            assigned = True
                        else:
                            logging.error(f"Reduce task {task_id} failed: {response.message}")
                            attempts += 1
                            
                    except Exception as e:
                        logging.error(f"Error assigning reduce task: {str(e)}")
                        attempts += 1
                        time.sleep(1)
                
                if not assigned:
                    logging.error(f"Failed to assign reduce task for key {key} after {max_attempts} attempts")
                    success = False
                    break

            return success

        except Exception as e:
            logging.error(f"Error in reduce phase: {str(e)}")
            return False

    def _monitor_workers(self):
        """Monitor worker health through heartbeats"""
        while True:
            current_time = time.time()
            dead_workers = []
            
            for worker_id, worker in self.workers.items():
                if current_time - worker.last_heartbeat > 15:  # 15 seconds timeout
                    logging.warning(f"Worker {worker_id} appears to be dead")
                    dead_workers.append(worker_id)
                    
            # Remove dead workers
            for worker_id in dead_workers:
                del self.workers[worker_id]
                
            time.sleep(5)

    def register_worker(self, worker_id: str, address: str):
        """Register a new worker"""
        worker = Worker(worker_id, address)
        self.workers[worker_id] = worker
        logging.info(f"Registered new worker {worker_id} at {address}")
        return worker

def serve(port):
    """Start the master server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master = MapReduceMaster()
    mapreduce_pb2_grpc.add_MapReduceServicer_to_server(master, server)
    
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    logging.info(f"Master server started on port {port}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("Usage: python master.py <port>")
        sys.exit(1)
        
    port = int(sys.argv[1])
    serve(port) 