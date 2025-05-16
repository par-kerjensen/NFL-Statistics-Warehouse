from pyspark.sql.functions import current_timestamp, lit, monotonically_increasing_id

def model(dbt, session):
    stats_df = dbt.ref('stg_tmp_spreadspoke_scores')
    stats_df = stats_df.withColumn("_load_time", lit(current_timestamp()))
    stats_df = stats_df.withColumn("game_id", monotonically_increasing_id())
    return stats_df