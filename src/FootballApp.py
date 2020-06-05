import sys

reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark.sql import SparkSession

from ManageData import *
from ManageDF import *
from WriteData import *

spark = SparkSession.builder.getOrCreate()


def main(argv):
    file_path = argv[1]
    df = load_csv_into_df(file_path)
    formated_df = format_df_data(df)
    filtered_df = filter_col_data(formated_df)
    df_with_is_played_home_col = add_column_is_played_home(filtered_df)
    stats_df = get_stats_df(df_with_is_played_home_col)
    stats_df.show()
    write_df_to_parquet_file(stats_df, 'stats.parquet')
    joined_df = join_df_on_field(filtered_df, stats_df)
    write_df_to_parquet_file(joined_df, 'results.parquet')


if __name__ == "__main__":
    main(sys.argv)
