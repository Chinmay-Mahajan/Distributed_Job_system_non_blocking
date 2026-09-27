import sys

def main():
    print("Reading numbers from stdin...")
    lines = sys.stdin.read().split()
    numbers = [int(x) for x in lines if x.strip()]
    numbers.sort()
    print(f"Sorted {len(numbers)} numbers:")
    print(numbers)
    print(f"Min: {numbers[0]}, Max: {numbers[-1]}")

if __name__ == "__main__":
    main()
