import sys

reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.types import BooleanType
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.getOrCreate()


def main(argv):
    file_path = argv[1]
    df = load_csv_into_df(file_path)
    formated_df = format_df_data(df)
    filtered_df = filter_col_data(formated_df)
    df_with_is_played_home_col = add_column_is_played_home(filtered_df)
    df_with_is_played_home_col.show()
    stats_df = get_stats_df(df_with_is_played_home_col)
    write_df_to_parquet_file(stats_df, 'stats.parquet')


# Import data from CSV into a Spark dataframe
def load_csv_into_df(file_path):
    df = spark.read.csv(file_path, header=True)
    # df = spark.read.csv("src/df_matches.csv", header=True)
    return df


# Format data with correct name and change NULL val to 0
def format_df_data(df):
    df = df.withColumnRenamed('X4', 'match').withColumnRenamed('X6', 'competition')
    casted_df = df.withColumn("penalty_france", df.penalty_france.cast('int')) \
        .withColumn("penalty_adversaire", df.penalty_adversaire.cast('int')) \
        .withColumn("score_adversaire", df.score_adversaire.cast('int')) \
        .withColumn("score_france", df.score_france.cast('int'))
    return casted_df.na.fill(0)


# keep only usefull col from df and with date > mars 1980
def filter_col_data(df):
    filtered_df = df.select('match', 'competition', 'adversaire', 'score_france', 'score_adversaire', 'penalty_france',
                            'penalty_adversaire', 'date')
    filtered_by_date_df = filtered_df.filter(filtered_df.date >= '1980-03-01')
    return filtered_by_date_df


def is_played_home(rencontre):
    if rencontre[:6] == 'France':
        return True
    else:
        return False


def is_world_cup(match_type):
    if match_type[:5] == 'Coupe':
        return 1
    else:
        return 0


# UDF calling is_played_home() checking if match is played at home, returns a bool
is_home_udf = F.udf(is_played_home, BooleanType())

# UDF calling is_world_cup() checking if match is from a world cup
is_world_cup_udf = F.udf(is_world_cup, IntegerType())


def add_column_is_played_home(df):
    df_with_is_played_home_col = df.withColumn("is_played_home", is_home_udf(df.match))
    return df_with_is_played_home_col


def get_stats_df(df):
    df_stat = (df
        .groupBy("adversaire")
        .agg(
        F.avg(df.score_france).alias("avg_score_france"),
        F.avg(df.score_adversaire).alias("avg_score_adversaire"),
        F.count(df.adversaire).alias("nb_match_jou-e-accent-aigu"),
        (F.sum(df.is_played_home.cast('int')) * 100 / F.count(df.adversaire)).alias('percent_home_played'),
        F.sum(is_world_cup_udf(df.competition)).alias('nb_mach_in_world_cup'),
        F.max(df.penalty_france).alias('max_penalty_france'),
        (F.sum(df.penalty_france) - F.sum(df.penalty_adversaire)).alias('dif_penalty')

    )
    )
    return df_stat


def write_df_to_parquet_file(df, file_name):
    df.write.parquet(file_name)


if __name__ == "__main__":
    main(sys.argv)
