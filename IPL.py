import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit

def main(session: snowpark.Session):

    
    df = session.table("BIGDATA_IPL_CLEAN_PY").select("OVER", "TOTAL_RUNS")

  
    stats = df.agg(
        {"OVER": "avg", "TOTAL_RUNS": "avg"}
    ).collect()[0]

    mean_x = stats[0]     # mean OVER
    mean_y = stats[1]     # mean TOTAL_RUNS

    
    var_cov = (
        df.select(
            ((col("OVER") - mean_x) * (col("OVER") - mean_x)).alias("var_x"),
            ((col("OVER") - mean_x) * (col("TOTAL_RUNS") - mean_y)).alias("cov_xy")
        )
        .agg({"var_x": "sum", "cov_xy": "sum"})
        .collect()[0]
    )

    var_x = var_cov[0]
    cov_xy = var_cov[1]

    
    m = cov_xy / var_x
    b = mean_y - m * mean_x

    
    df_pred = df.with_column(
        "PREDICTED_RUNS", 
        m * col("OVER") + b
    )

    
    df_pred.write.mode("overwrite").save_as_table("IPL_PREDICTED_RUNS")

    
    df_pred.show()

    return df_pred
