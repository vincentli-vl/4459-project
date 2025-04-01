def create_test_file(filename, size_mb=1):
    """Create a test file with random text data"""
    import random
    
    words = ["apple", "banana", "orange", "grape", "mango", 
             "python", "java", "golang", "rust", "javascript",
             "distributed", "systems", "computing", "data", "processing"]
    
    # Calculate approximate number of lines needed for desired file size
    # Assuming average word length of 6 characters + space
    chars_per_mb = 1024 * 1024
    words_per_line = 10
    lines_needed = (size_mb * chars_per_mb) // (words_per_line * 7)
    
    with open(filename, 'w') as f:
        for _ in range(lines_needed):
            line = ' '.join(random.choices(words, k=words_per_line))
            f.write(line + '\n')

if __name__ == '__main__':
    create_test_file('test_input.txt', size_mb=1) 