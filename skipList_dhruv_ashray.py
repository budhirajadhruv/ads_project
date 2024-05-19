import random
import time
import sys
from tqdm import tqdm # type: ignore

class SkipNode:
    def __init__(self, key, level, values):
        self.key = key
        self.values = values
        self.forward = [None] * (level + 1)

class SkipList:
    def __init__(self, max_level=16, p=0.5):
        self.max_level = max_level
        self.p = p
        self.header = self.create_node(self.max_level, None, None)
        self.level = 0
        self.space_taken = sys.getsizeof(self.header)
        self.size = 0

    def create_node(self, level, key=None, values=None):
        return SkipNode(key, level, values)

    def random_level(self):
        level = 0
        while random.random() < self.p and level < self.max_level:
            level += 1
        return level

    def insert(self, key):
        update = [None] * (self.max_level + 1)
        current = self.header

        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current

        current = current.forward[0]
        if current and current.key == key:
            return

        rlevel = self.random_level()
        if rlevel > self.level:
            for i in range(self.level + 1, rlevel + 1):
                update[i] = self.header
            self.level = rlevel

        values = [random.randint(0, 100) for _ in range(3)]
        new_node = self.create_node(rlevel, key, values)
        for i in range(rlevel + 1):
            new_node.forward[i] = update[i].forward[i]
            update[i].forward[i] = new_node

        self.size += 1
        self.space_taken += sys.getsizeof(new_node) + sys.getsizeof(values)

    def delete(self, key: int) -> bool:
        update = [None] * (self.max_level + 1)
        current = self.header
        deleted_space = 0

        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current

        current = current.forward[0]
        if current and current.key == key:
            for i in range(self.level + 1):
                if update[i].forward[i] != current:
                    break
                update[i].forward[i] = current.forward[i]

            while self.level > 0 and self.header.forward[self.level] is None:
                self.level -= 1

            deleted_space += sys.getsizeof(current) + sys.getsizeof(current.values)
            self.space_taken -= deleted_space
            self.size -= 1
            return True
        else:
            return False

    def search(self, key):
        current = self.header
        for i in range(self.level, 1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]

        current = current.forward[0]
        if current and current.key == key:
            return current.values

        return None

    def display_list(self):
        # print("Skip List Levels: ", self.level)
        for lvl in range(self.level + 1):
            node = self.header.forward[lvl]
            line = ""
            while node:
                line += f"Key: {node.key}, Values: {node.values} "
                node = node.forward[lvl]
            # print("Level " + str(lvl) + ": " + line)

    def free(self):
        print("Space taken: ", self.space_taken, " bytes")
        self.header = self.create_node(self.max_level, None, None)
        self.level = 0
        self.space_taken = sys.getsizeof(self.header)
        self.size = 0

    def range_query(self, low, high):
        result = []
        current = self.header
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < low:
                current = current.forward[i]

        current = current.forward[0]
        while current and low <= current.key <= high:
            result.append((current.key, current.values))
            current = current.forward[0]
        size = sys.getsizeof(result)
        print('The space for range query' , size ,"bytes")
        return result
    
skip_list = SkipList()

num_values_to_insert = int(input("Enter the number of random values to insert: "))
insertion_space = 0
start_time = time.time()
with tqdm(total=num_values_to_insert) as pbar:
    for _ in range(num_values_to_insert):
        random_key = random.randint(0, 5000) #change
        #random_key = random.randint(0, 500)
        skip_list.insert(random_key)
        pbar.update(1)
end_time = time.time()
skip_list.display_list()
print()
print("Time taken to insert", num_values_to_insert, "random values: ", end_time - start_time, "seconds")
print("Total space taken after insertions: ", skip_list.space_taken, "bytes")
# print("Total size of the skip list: ", skip_list.size)
print()


print()
num_values_to_delete = int(input("Enter the number of random values to delete: "))

deleted_values = []
deleted_space = 0
start_time = time.time()
with tqdm(total=num_values_to_delete) as pbar:
    for _ in range(num_values_to_delete):
        key_to_delete = random.randint(0, 1000) #change
        if skip_list.delete(key_to_delete):
            deleted_values.append(key_to_delete)
  # Assuming key size is representative of node size
        pbar.update(1)
end_time = time.time()
print("Deleted values: ", deleted_values)
print("Time taken to delete multiple values: ", end_time - start_time, "seconds")
# print("Total size of the skip list after deletion: ", skip_list.size)
print("Space taken after deletion: ", skip_list.space_taken, "bytes")


num_values_to_search = int(input("Enter the number of random values to search: "))
start_time = time.time()
results=[]
with tqdm(total=num_values_to_search) as pbar:
    for _ in range(num_values_to_search):
        search_value = random.randint(0, 1500) #change
        search_result = skip_list.search(search_value)
        if search_result:
            results.append(search_value)
            print(f"Value {search_value} Found with values: {search_result}")
        else:
            print(f"Value {search_value} NOT FOUND")
        pbar.update(1)
end_time = time.time()
print("Time taken to search for multiple values: ", end_time - start_time, "seconds")
size = sys.getsizeof(results)
print('The space for Search' , size ,"bytes")

print()

range_low = int(input("Enter the lower limit of the range query: "))
range_high = int(input("Enter the upper limit of the range query: "))
start_time = time.time()
range_result = skip_list.range_query(range_low, range_high)
# print(f"Values between {range_low} and {range_high}: {range_result}")
print()
end_time = time.time()
print("Time taken for range query: ", end_time - start_time, "seconds")



skip_list.free()
print("Skip List Freed")