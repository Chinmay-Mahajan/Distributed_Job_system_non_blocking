import time
import sys

print("Starting an infinite loop that will never terminate on its own...")
sys.stdout.flush()

count = 0
while True:
    time.sleep(1)
    count += 1
    print(f"Still running after {count} seconds...")
    sys.stdout.flush()
