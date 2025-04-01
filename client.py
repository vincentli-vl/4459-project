import grpc, sys
import mapreduce_pb2, mapreduce_pb2_grpc

def run():
    # Check if the user entered the corrent number of arguments
    if len(sys.argv) != 3:
        print("In order to run this file: python client.py <job_name> <input_file>")
        return
    
    # Use the arguments and declare the request variables
    job_name = sys.argv[1]
    input_file = sys.argv[2]
    
    channel = grpc.insecure_channel('localhost:50051')
    stub = mapreduce_pb2_grpc.MapReduceStub(channel)
    
    # Save the request object in a variable for future call
    request = mapreduce_pb2.JobRequest(
        job_name=job_name,
        input_file=input_file
    )
    
    # Call the server with the request object and print the results depending if it fails or not
    try:
        response = stub.SubmitJob(request)
        
        if response.success:
            print(f"[Client]: Job {job_name} submitted successfully!")
            print(f"[Client]: Message: {response.message}")
            print(f"[Client]: Job ID: {response.job_id}")
        else:
            print(f"[Client]: Job {job_name} failed to submit!")
            print(f"[Client]: Message: {response.message}")
            
    except grpc.RpcError as e:
        print(f"[Client]: Error occurred: {e}")
        
if __name__ == '__main__':
    run()