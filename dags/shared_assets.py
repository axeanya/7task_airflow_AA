from airflow.sdk.definitions.asset import Asset

aa_asset = Asset(
    name="trainee_asset",
    uri="file:///localhost/opt/airflow/share/tiktok_google_play_reviews_upd.csv",
)
