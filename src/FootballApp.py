import sys
from pyspark.sql import SparkSession

def main(argv):
    print(argv[1])

if __name__ == "__main__":
    main(sys.argv)