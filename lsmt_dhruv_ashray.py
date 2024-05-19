import time
import random
import os
from pathlib import Path
from tqdm import tqdm # type: ignore
import sys

class SSTable:
    def __init__(self, directory, id):
        self.directory = directory
        self.id = id
        self.path = f"{directory}/ss_table_{id}.txt"
        self.data = []
        self.deletion_log = set()

    def write_to_disk(self):
        with open(self.path, 'w') as f:
            for key, value in self.data:
                if value is not None:
                    f.write(f"{key},{value}\n")
        self.data.clear()

    def read_from_disk(self):
        with open(self.path, 'r') as f:
            for line in f:
                key, value = line.strip().split(',')
                yield int(key), value

    def delete(self):
        os.remove(self.path)

class LSMTree:
    def __init__(self, memtable_limit=0, max_sstables=5):
        self.memtable = {}
        self.sstables = []
        self.memtable_limit = memtable_limit
        self.max_sstables = max_sstables
        self.directory = Path("sstables")
        self.directory.mkdir(exist_ok=True)
        self.sstable_counter = 0
        self.deletion_log = set()
        self.search_results = {}
        self.range_search= {}

    def insert(self, key, value):
        if key in self.deletion_log:
            self.deletion_log.remove(key)
        self.memtable[key] = value
        if len(self.memtable) >= self.memtable_limit:
            self.flush_memtable()

    def delete(self, key):
        self.deletion_log.add(key)
        if key in self.memtable:
            del self.memtable[key]

    def delete_range(self, start_key, end_key):
        for key in tqdm(range(start_key, end_key + 1), desc="Deleting range"):
            self.delete(key)

    def flush_memtable(self):
        # Sort the memtable data based on keys before flushing
        sorted_memtable = sorted(self.memtable.items(), key=lambda x: x[0])
        
        new_sstable = SSTable(self.directory, self.sstable_counter)
        new_sstable.data = [(k, v) for k, v in sorted_memtable if k not in self.deletion_log]
        new_sstable.write_to_disk()
        self.sstables.append(new_sstable)
        self.memtable.clear()
        self.sstable_counter += 1
        self.compact_sstables()

    def compact_sstables(self):
        if len(self.sstables) > self.max_sstables:
            merged_data = {}
            for sstable in self.sstables:
                for key, value in sstable.read_from_disk():
                    if key not in self.deletion_log:
                        merged_data[key] = value
            for sstable in self.sstables:
                sstable.delete()
            self.sstables.clear()
            self.deletion_log.clear()
            sorted_items = sorted(merged_data.items())
            chunks = [sorted_items[i:i + self.memtable_limit] for i in range(0, len(sorted_items), self.memtable_limit)]
            for chunk in chunks:
                new_sstable = SSTable(self.directory, self.sstable_counter)
                new_sstable.data = chunk
                new_sstable.write_to_disk()
                self.sstables.append(new_sstable)
                self.sstable_counter += 1

    def enforce_final_compaction(self):
        if len(self.sstables) > self.max_sstables:
            self.compact_sstables()

    def get_memtable_count(self):
        return len(self.memtable)
    
    def user_find_interface(self):
        num_keys = int(input("Enter the number of keys to find: "))
        keys = []
        for _ in range(num_keys):
            key = int(input("Enter a key to find: "))
            keys.append(key)

        start_time = time.time()
        results = []
        for key in keys:
            result = self.find(key)
            results.append(result)
        elapsed_time = time.time() - start_time
        print(f"Time taken to find all keys: {elapsed_time:.4f} seconds")

    def search_sstable(self, sstable, key):
        try:
            with open(sstable.path, 'r') as file:
                for line in file:
                    k, v = line.strip().split(',')
                    k = int(k)
                    if k == key:
                        print(f"Found key {key} in SSTable {sstable.id}")
                        return v
                    elif k > key:
                        break
        except Exception as e:
            print(f"Error reading SSTable: {e}")
        return None

    def find(self, key):
        if key in self.memtable:
            self.search_results[key] = self.memtable[key]
            print(f"Found key {key} in memory")
            return self.memtable[key]
        for sstable in reversed(self.sstables):
            result = self.search_sstable(sstable, key)
            if result is not None:
                self.search_results[key] = result
                return result
        print(f"Key {key} not found")
        return "Key not found"

    def range_query(self, start_key, end_key):
        start_time = time.time() 
        results = {}
        for key, value in self.memtable.items():
            if start_key <= key <= end_key:
                results[key] = value
        for sstable in self.sstables:
            for key, value in sstable.read_from_disk():
                if start_key <= key <= end_key and key not in results:
                    results[key] = value
        end_time = time.time()
        time_taken = end_time - start_time
        print(f"Time taken for range query: {time_taken:.4f} seconds")
        return sorted(results.items())
    
    def print_memory_usage(self):
        size = sys.getsizeof(self.search_results)
        print(f"Space used by search results: {size} bytes")

def main():
    lsm_tree = LSMTree(memtable_limit=300000, max_sstables=10)
    start_time = time.time()
    random_keys = random.sample(range(10000000), 10000000)  # Generate a list of unique random keys #change
    for key in tqdm(random_keys):
        lsm_tree.insert(key, f"value{key}")
    print(f"Time taken for insertion: {time.time() - start_time:.4f} seconds")

    

    # Get the current directory
    current_directory = os.getcwd()

    # Initialize total size
    total_size = 0

    # Walk through the directory tree
    for dirpath, dirnames, filenames in os.walk(current_directory):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    k=total_size

    print(f"Total space for insertion: {total_size} bytes")


    start_time = time.time()
    lsm_tree.delete_range(1, 20)
    print(f"Time taken for range deletion: {time.time() - start_time:.4f} seconds")
    lsm_tree.enforce_final_compaction()
    # Get the current directory
    current_directory = os.getcwd()

    # Initialize total size
    total_size = 0

    # Walk through the directory tree
    for dirpath, dirnames, filenames in os.walk(current_directory):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)

    print(f"Total space for deletion: {k-total_size} bytes")




    
    ###search
    lsm_tree.user_find_interface()
    lsm_tree.print_memory_usage()





#range query code    
    start_key = 2000
    end_key = 50000
    results = lsm_tree.range_query(start_key, end_key)
    size = sys.getsizeof(results)
    print(f"Space used by range query: {size} bytes")
    print("Range Query Results:")
    

    
    ff=input('Press Enter for range Query')
    for key, value in results:
        print(f"Key: {key}, Value: {value}")

    
    


    ##data in memory
    memtable_count = lsm_tree.get_memtable_count()
    print(f"Current number of entries in MemTable: {memtable_count}")
    

if __name__ == "__main__":
    main()















