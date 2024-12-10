def model(dbt, session):
    stg_customrers_df = dbt.source("qwt_raw","customers")
    return stg_customrers_df