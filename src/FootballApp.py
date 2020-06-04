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
    filtered_df = filter_col_data(formated_df)
    filtered_df.show()


# Import data from CSV into a Spark dataframe
def load_csv_into_df():
    df = spark.read.csv("src/df_matches.csv", header=True)
    return df

#Format data with correct name and change NULL val to 0
def format_df_data(df):
    df = df.withColumnRenamed('X4', 'match').withColumnRenamed('X6', 'competition')
    casted_df = df.withColumn("penalty_france", df.penalty_france.cast('int'))\
        .withColumn("penalty_adversaire", df.penalty_adversaire.cast('int'))\
        .withColumn("score_adversaire", df.score_adversaire.cast('int'))\
        .withColumn("score_france", df.score_france.cast('int'))
    return casted_df.na.fill(0)

#keep only usefull col from df and with date > mars 1980
def filter_col_data(df):
    filtered_df = df.select('match', 'competition', 'adversaire', 'score_france', 'score_adversaire', 'penalty_france', 'penalty_adversaire', 'date')
    filtered_by_date_df = filtered_df.filter(filtered_df.date > '1980-03-01')
    return filtered_by_date_df


if __name__ == "__main__":
    main(sys.argv)
