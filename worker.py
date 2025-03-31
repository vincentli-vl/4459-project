import grpc
import time
import mapreduce_pb2
import mapreduce_pb2_grpc

# This is the map function that will be called by the worker.
# Splits input text into words and emits (word, 1) pairs.
def map_function(text):
    result = []
    words = text.strip().split()
    for word in words:
        word = word.lower().strip(",.?!;:()[]{}'\"")
        if word:
            result.append(mapreduce_pb2.KeyValue(key=word, value=1))
    return result

# This is the reduce function that will be called by the worker.
# It takes a key and a list of values and sums them up.
# For example, if the key is "word" and the values are [1, 1, 1], it will return (word, 3).
def reduce_function(key, values):
    return sum(values)

class Worker:
    def __init__(self, master_address="localhost:50051"):
        self.master_address = master_address
        self.channel = grpc.insecure_channel(self.master_address)
        self.stub = mapreduce_pb2_grpc.MasterStub(self.channel)
        
    def run(self):
        print("[Worker]: Worker started. Polling for map tasks...")
        while True:
            try:
                # Request a map task from the master
                map_task = self.stub.AssignMapTask(mapreduce_pb2.Empty())
                
                if map_task.input_chunk != "":
                    print(f"[Worker]: Received map task {map_task.task_id}")
                    kv_pairs = map_function(map_task.input_chunk)

                    map_result = mapreduce_pb2.MapResult(
                        task_id=map_task.task_id,
                        kv_pairs=kv_pairs
                    )

                    ack = self.stub.SubmitMapResult(map_result)
                    if ack.success:
                        print(f"[Worker]: Map task {map_task.task_id} completed successfully.")
                    else:
                        print(f"[Worker]: Map task {map_task.task_id} failed: {ack.message}")

                else:
                    # Request a reduce task from the master
                    reduce_task = self.stub.AssignReduceTask(mapreduce_pb2.Empty())

                    if reduce_task.key != "":
                        print(f"[Worker]: Received reduce task {reduce_task.task_id} for key '{reduce_task.key}'")

                        reduced_value = reduce_function(reduce_task.key, reduce_task.values)

                        reduce_result = mapreduce_pb2.ReduceResult(
                            task_id=reduce_task.task_id,
                            key=reduce_task.key,
                            result=reduced_value
                        )

                        ack = self.stub.SubmitReduceResult(reduce_result)

                        if ack.success:
                            print(f"[Worker]: Reduce task {reduce_task.task_id} completed successfully.")
                        else:
                            print(f"[Worker]: Reduce task {reduce_task.task_id} failed: {ack.message}")
                    else:
                        print("[Worker]: No map or reduce tasks available. Sleeping.")
                        time.sleep(3)
                    
            except grpc.RpcError as e:
                print(f"[Worker]: gRPC error: {e.code()} - {e.details()}")
                time.sleep(3)
                
if __name__ == '__main__':
    worker = Worker()
    worker.run()
