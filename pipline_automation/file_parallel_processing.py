from multiprocessing import Pool

# Simulated data
data = [i for i in range(100)]

# Processing function
def process_chunk(chunk):
    return sum(chunk)

# Parallel processing
def parallel_process(data, chunk_size):
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    with Pool(processes=4) as pool:
        results = pool.map(process_chunk, chunks)
    return results

results = parallel_process(data, chunk_size=10)
print(f"Processed Results: {results}")