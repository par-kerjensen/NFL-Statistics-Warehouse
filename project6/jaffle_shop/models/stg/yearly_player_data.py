from pyspark.sql.functions import current_timestamp, lit, monotonically_increasing_id

def model(dbt, session):
    stats_df = dbt.ref('stg_tmp_yearly_player_data')
    stats_df = stats_df.withColumn("player_season_id", monotonically_increasing_id())
    return stats_df