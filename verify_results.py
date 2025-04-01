import logging
import sys
import os
import glob

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def verify_results(job_id):
    """Verify the results of a MapReduce job"""
    output_dir = f"/tmp/mapreduce/{job_id}"
    
    # Check if output directory exists
    if not os.path.exists(output_dir):
        logging.error(f"Output directory not found: {output_dir}")
        return False
        
    logging.info(f"Checking results in {output_dir}")
    word_counts = {}
    total_mapreduce_count = 0
    
    # Read all reduce output files
    reduce_files = glob.glob(f"{output_dir}/reduce_*")
    if not reduce_files:
        logging.error("No reduce output files found")
        return False
        
    logging.info(f"Found {len(reduce_files)} reduce output files")
    
    # Read reduce results
    for filename in reduce_files:
        try:
            with open(filename, 'r') as f:
                for line in f:
                    word, count = line.strip().split('\t')
                    count = int(count)
                    word_counts[word] = count
                    total_mapreduce_count += count
        except Exception as e:
            logging.error(f"Error reading {filename}: {str(e)}")
            return False
    
    # Count words directly from input file
    if not os.path.exists('test_input.txt'):
        logging.error("test_input.txt not found")
        return False
        
    logging.info("Counting words in original input file")
    direct_counts = {}
    total_direct_count = 0
    
    try:
        with open('test_input.txt', 'r') as f:
            for line in f:
                for word in line.strip().split():
                    word = word.lower()
                    direct_counts[word] = direct_counts.get(word, 0) + 1
                    total_direct_count += 1
    except Exception as e:
        logging.error(f"Error reading test_input.txt: {str(e)}")
        return False
    
    # Compare results
    logging.info(f"\nResults Summary:")
    logging.info(f"Total words processed by MapReduce: {total_mapreduce_count}")
    logging.info(f"Total words in original file: {total_direct_count}")
    logging.info(f"Unique words found: {len(word_counts)}")
    
    if word_counts == direct_counts:
        logging.info("\nResults verified successfully! All word counts match.")
        logging.info("\nWord frequency counts:")
        for word, count in sorted(word_counts.items()):
            logging.info(f"{word}: {count}")
        return True
    else:
        logging.error("\nResults verification failed!")
        logging.error("Differences found:")
        
        all_words = sorted(set(word_counts) | set(direct_counts))
        differences = 0
        for word in all_words:
            mr_count = word_counts.get(word, 0)
            dir_count = direct_counts.get(word, 0)
            if mr_count != dir_count:
                differences += 1
                logging.error(f"Word: {word}")
                logging.error(f"  MapReduce count: {mr_count}")
                logging.error(f"  Direct count: {dir_count}")
                logging.error(f"  Difference: {mr_count - dir_count}")
        
        logging.error(f"\nTotal differences found: {differences} words")
        return False

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python verify_results.py <job_id>")
        print("Example: python verify_results.py 550e8400-e29b-41d4-a716-446655440000")
        sys.exit(1)
    
    job_id = sys.argv[1]
    success = verify_results(job_id)
    sys.exit(0 if success else 1) 