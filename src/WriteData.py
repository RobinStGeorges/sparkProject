import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# input : df
# write the given DF as a parquet file with the given name
def write_df_to_parquet_file(df, file_name):
    df.write.mode("overwrite").parquet(file_name)
