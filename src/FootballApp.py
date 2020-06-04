import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

def main(argv):
    print(argv[1])
    df = load_csv_into_df()
    df.show()

#Import data from CSV into a Spark dataframe
def load_csv_into_df():
    df = spark.read.csv("src/df_matches.csv")
    df.printSchema()
    return df


if __name__ == "__main__":
    main(sys.argv)