from pyspark.sql.functions import monotonically_increasing_id

def model(dbt, session):
    stats_df = dbt.ref('stg_tmp_yearly_team_data')
    stats_df = stats_df.withColumn("team_season_id", monotonically_increasing_id())
    return stats_df