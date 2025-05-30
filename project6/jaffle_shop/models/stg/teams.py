from pyspark.sql.functions import current_timestamp, lit

def model(dbt, session):
    stats_df = dbt.ref('stg_tmp_teams')
    stats_df = stats_df.withColumn("_load_time", lit(current_timestamp()))
    return stats_df