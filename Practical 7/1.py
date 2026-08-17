import threading
import time
import random

print("NAME : RISHI PAL")
print("ROLL NO : S100\n")

BUFFER_SIZE = 5
buffer = [None] * BUFFER_SIZE
in_idx = 0
out_idx = 0

mutex = threading.Lock()
empty = threading.Semaphore(BUFFER_SIZE)
full = threading.Semaphore(0)

def producer(items_to_produce):
    global in_idx
    for i in range(items_to_produce):
        item = f"Data-{i+1}"
        
        time.sleep(random.uniform(0.1, 0.4))
        
        empty.acquire()
        mutex.acquire()
        
        buffer[in_idx] = item
        print(f"[+] Produced: {item} at index {in_idx} | Buffer: {buffer}")
        in_idx = (in_idx + 1) % BUFFER_SIZE
        
        mutex.release()
        full.release()

def consumer(items_to_consume):
    global out_idx
    for _ in range(items_to_consume):
        full.acquire()
        mutex.acquire()
        
        item = buffer[out_idx]
        buffer[out_idx] = None
        print(f"[-] Consumed: {item} from index {out_idx} | Buffer: {buffer}")
        out_idx = (out_idx + 1) % BUFFER_SIZE
        
        mutex.release()
        empty.release()
        
        time.sleep(random.uniform(0.2, 0.5))

if __name__ == "__main__":
    TOTAL_ITEMS = 10
    
    prod_thread = threading.Thread(target=producer, args=(TOTAL_ITEMS,))
    cons_thread = threading.Thread(target=consumer, args=(TOTAL_ITEMS,))
    
    prod_thread.start()
    cons_thread.start()
    
    prod_thread.join()
    cons_thread.join()
    
    print("\nProcessing complete.")
