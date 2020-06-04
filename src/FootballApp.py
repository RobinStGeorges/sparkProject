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
    formated_df = format_df_data(df)
    formated_df.show()


# Import data from CSV into a Spark dataframe
def load_csv_into_df():
    df = spark.read.csv("src/df_matches.csv", header=True)
    return df


def format_df_data(df):
    df = df.withColumnRenamed('X4', 'match').withColumnRenamed('X6', 'competition')
    casted_df = df.withColumn("penalty_france", df.penalty_france.cast('int')).withColumn("penalty_adversaire", df.penalty_adversaire.cast('int'))
    casted_df.show()
    return casted_df.na.fill(0)


if __name__ == "__main__":
    main(sys.argv)
