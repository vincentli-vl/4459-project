# 4459-project

## How to run

### Option 1: using the client
1. Run `python test_mapreduce.py`
2. Verify results by running `python verify_results <job_id>`

### Option 2: manually testing
1. Make sure the test_input.txt file exists, if not, run `python create_test_file.py` to create a text file of words
2. Run `python master.py 50051` 
3. Create the clients in another terminal: `python worker.py worker1 50052`, `python worker.py worker2 50053`, etc. Increment the workers and port by 1 after the master (ex. 50052 -> 50053 -> 50054 -> ...)
4. Run the client with `python client.py word_count test_input.txt`
5. Verify results with `python verify_results <job_id>`