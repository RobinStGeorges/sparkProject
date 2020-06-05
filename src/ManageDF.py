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


# input : string
# output : boolean
def is_played_home(rencontre):
    if rencontre[:6] == 'France':
        return True
    else:
        return False


# input : string
# output : int
def is_world_cup(match_type):
    if match_type[:5] == 'Coupe':
        return 1
    else:
        return 0


# UDF calling is_played_home() checking if match is played at home, returns a bool
is_home_udf = F.udf(is_played_home, BooleanType())

# UDF calling is_world_cup() checking if match is from a world cup
is_world_cup_udf = F.udf(is_world_cup, IntegerType())


# input : df
# output : df
def add_column_is_played_home(df):
    df_with_is_played_home_col = df.withColumn("is_played_home", is_home_udf(df.match))
    return df_with_is_played_home_col


# input : df
# output : df
# make stats for part 2 with the df from part 1
def get_stats_df(df):
    df_stat = (df
        .groupBy("adversaire")
        .agg(
        F.avg(df.score_france).alias("avg_score_france"),
        F.avg(df.score_adversaire).alias("avg_score_adversaire"),
        F.count(df.adversaire).alias("nb_match_joue"),
        (F.sum(df.is_played_home.cast('int')) * 100 / F.count(df.adversaire)).alias('percent_home_played'),
        F.sum(is_world_cup_udf(df.competition)).alias('nb_mach_in_world_cup'),
        F.max(df.penalty_france).alias('max_penalty_france'),
        (F.sum(df.penalty_france) - F.sum(df.penalty_adversaire)).alias('dif_penalty')
    )
    )
    return df_stat


def join_df(df1, df2):
    df2 = df2.withColumnRenamed("adversaire", "adversaire_name")

    joined_df = df1.join(
        df2, df1.adversaire == df2.adversaire_name
    )
    joined_df = joined_df.drop('adversaire_name')
    return joined_df
