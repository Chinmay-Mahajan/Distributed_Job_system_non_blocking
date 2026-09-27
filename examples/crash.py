import sys

def faulty_function():
    print("Doing some work before failing...")
    # Deliberate division by zero error
    return 100 / 0

if __name__ == "__main__":
    faulty_function()
