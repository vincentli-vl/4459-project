import grpc
import time
import threading
import mapreduce_pb2
import mapreduce_pb2_grpc
from concurrent import futures
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class MapReduceWorker:
    def __init__(self, worker_id, port, master_address='localhost:50051'):
        self.worker_id = worker_id
        self.port = port
        self.master_address = master_address
        self.is_active = True
        self.current_task = None
        self.status = "IDLE"
        
        # Initialize thread for heartbeat
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeat)
        self.heartbeat_thread.daemon = True

    def start(self):
        """Start the worker and heartbeat mechanism"""
        logging.info(f"Worker {self.worker_id} starting...")
        self.heartbeat_thread.start()
        
    def map_task(self, data_chunk):
        """Execute map task on the given data chunk"""
        logging.info(f"Worker {self.worker_id} executing map task")
        self.status = "MAPPING"
        
        try:
            # Initialize result dictionary for key-value pairs
            mapped_data = []
            
            # Process each line in the data chunk
            for line in data_chunk.split('\n'):
                if line.strip():
                    # Split words and emit (word, 1) pairs
                    words = line.strip().split()
                    for word in words:
                        mapped_data.append((word.lower(), 1))
            
            logging.info(f"Map task completed successfully")
            return mapped_data
            
        except Exception as e:
            logging.error(f"Error in map task: {str(e)}")
            raise
        finally:
            self.status = "IDLE"

    def reduce_task(self, mapped_data):
        """Execute reduce task on the mapped data"""
        logging.info(f"Worker {self.worker_id} executing reduce task")
        self.status = "REDUCING"
        
        try:
            # Dictionary to store word counts
            reduced_data = {}
            
            # Combine values for each key
            for key, value in mapped_data:
                if key in reduced_data:
                    reduced_data[key] += value
                else:
                    reduced_data[key] = value
            
            logging.info(f"Reduce task completed successfully")
            return reduced_data
            
        except Exception as e:
            logging.error(f"Error in reduce task: {str(e)}")
            raise
        finally:
            self.status = "IDLE"

    def _send_heartbeat(self):
        """Send periodic heartbeat to master node"""
        while self.is_active:
            try:
                with grpc.insecure_channel(self.master_address) as channel:
                    stub = mapreduce_pb2_grpc.MapReduceStub(channel)
                    request = mapreduce_pb2.HeartbeatRequest(
                        worker_id=self.worker_id,
                        status=self.status,
                        current_task=self.current_task if self.current_task else "none",
                        address=f"localhost:{self.port}"
                    )
                    response = stub.SendHeartbeat(request)
                    if response.acknowledged:
                        logging.debug(f"Heartbeat acknowledged: {response.message}")
                    else:
                        logging.warning(f"Heartbeat not acknowledged: {response.message}")
            except Exception as e:
                logging.error(f"Error sending heartbeat: {str(e)}")
            finally:
                time.sleep(5)  # Send heartbeat every 5 seconds

    def shutdown(self):
        """Gracefully shutdown the worker"""
        logging.info(f"Worker {self.worker_id} shutting down...")
        self.is_active = False
        if self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join()

class WorkerServicer(mapreduce_pb2_grpc.WorkerServicer):
    def AssignMapTask(self, request, context):
        """Handle map task assignment"""
        logging.info(f"Received map task: {request.task_id}")
        try:
            # Process the data chunk
            mapped_data = []
            word_count = 0
            
            for line in request.data_chunk.split('\n'):
                if line.strip():
                    # Split words and emit (word, 1) pairs
                    words = line.strip().split()
                    word_count += len(words)
                    for word in words:
                        # Create KeyValuePair for each word
                        kvp = mapreduce_pb2.KeyValuePair(
                            key=word.lower(),
                            value=1
                        )
                        mapped_data.append(kvp)
            
            logging.info(f"Processed {word_count} words in map task")
            
            # Ensure output directory exists
            output_dir = os.path.dirname(request.output_location)
            os.makedirs(output_dir, exist_ok=True)
            
            # Write intermediate results to file
            with open(request.output_location, 'w') as f:
                for kvp in mapped_data:
                    f.write(f"{kvp.key}\t{kvp.value}\n")
            
            logging.info(f"Wrote {len(mapped_data)} key-value pairs to {request.output_location}")
            
            return mapreduce_pb2.TaskResponse(
                success=True,
                task_id=request.task_id,
                message=f"Map task completed successfully. Processed {word_count} words",
                result=mapped_data
            )
            
        except Exception as e:
            logging.error(f"Error in map task: {str(e)}")
            return mapreduce_pb2.TaskResponse(
                success=False,
                task_id=request.task_id,
                message=f"Map task failed: {str(e)}"
            )

    def AssignReduceTask(self, request, context):
        """Handle reduce task assignment"""
        logging.info(f"Received reduce task: {request.task_id}")
        try:
            # Group and sum values by key
            reduced_data = {}
            count = 0
            
            # Process all mapped data
            for kvp in request.mapped_data:
                count += 1
                if kvp.key in reduced_data:
                    reduced_data[kvp.key] += kvp.value
                else:
                    reduced_data[kvp.key] = kvp.value
            
            logging.info(f"Processed {count} key-value pairs in reduce task")
            
            # Convert reduced data back to KeyValuePairs
            result = []
            for key, value in reduced_data.items():
                kvp = mapreduce_pb2.KeyValuePair(
                    key=key,
                    value=value
                )
                result.append(kvp)
                logging.info(f"Reduced result for key '{key}': {value}")
            
            # Ensure output directory exists
            os.makedirs(os.path.dirname(request.output_location), exist_ok=True)
            
            # Write results to output file
            with open(request.output_location, 'w') as f:
                for kvp in result:
                    f.write(f"{kvp.key}\t{kvp.value}\n")
            
            logging.info(f"Wrote {len(result)} results to {request.output_location}")
            
            return mapreduce_pb2.TaskResponse(
                success=True,
                task_id=request.task_id,
                message=f"Reduce task completed successfully. Processed {count} pairs",
                result=result
            )
            
        except Exception as e:
            logging.error(f"Error in reduce task: {str(e)}")
            return mapreduce_pb2.TaskResponse(
                success=False,
                task_id=request.task_id,
                message=f"Reduce task failed: {str(e)}"
            )

def serve(worker_id, port, master_port=50051):
    """Start the worker server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Create worker instance with master address and worker port
    worker = MapReduceWorker(worker_id, port, f"localhost:{master_port}")
    worker_servicer = WorkerServicer()
    
    mapreduce_pb2_grpc.add_WorkerServicer_to_server(worker_servicer, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    # Start the worker (this will start the heartbeat)
    worker.start()
    
    logging.info(f"Worker {worker_id} started on port {port}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        worker.shutdown()
        server.stop(0)

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print("Usage: python worker.py <worker_id> <port>")
        sys.exit(1)
    
    worker_id = sys.argv[1]
    port = int(sys.argv[2])
    serve(worker_id, port)
