import io
import logging
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET = "de300-airflow-yi-barnett"
RAW_PREFIX = "ml-1m"             
EMB_KEY = "embeddings/movie_embeddings.parquet"
MODEL_NAME = "all-MiniLM-L6-v2"   

log = logging.getLogger(__name__)

default_args = {"owner": "de300_group", "retries": 1}


def _s3():
    import boto3
    return boto3.client("s3")


def _exists(client, bucket, key):
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def generate_movie_embeddings(**_):
    import pandas as pd
    from sentence_transformers import SentenceTransformer

    client = _s3()
    if _exists(client, BUCKET, EMB_KEY):
        log.info("Embeddings already exist at s3://%s/%s -- skipping.", BUCKET, EMB_KEY)
        return

    obj = client.get_object(Bucket=BUCKET, Key=f"{RAW_PREFIX}/movies.dat")
    movies = pd.read_csv(
        io.BytesIO(obj["Body"].read()),
        sep="::", engine="python", encoding="latin-1",
        names=["MovieID", "Title", "Genres"],
    )

    movies["text"] = movies["Title"] + " | " + movies["Genres"].str.replace("|", " ", regex=False)

    model = SentenceTransformer(MODEL_NAME)
    vecs = model.encode(movies["text"].tolist(), show_progress_bar=False, batch_size=64)

    emb = pd.DataFrame(vecs, index=movies["MovieID"])
    emb.columns = [f"d{i}" for i in range(emb.shape[1])]
    emb = emb.reset_index()

    buf = io.BytesIO()
    emb.to_parquet(buf, index=False)
    buf.seek(0)
    client.put_object(Bucket=BUCKET, Key=EMB_KEY, Body=buf.getvalue())
    log.info("Wrote %d movie embeddings to s3://%s/%s", len(emb), BUCKET, EMB_KEY)


with DAG(
    dag_id="movie_embeddings",
    default_args=default_args,
    description="Offline BERT embeddings for all movies (run once).",
    start_date=pendulum.datetime(2026, 6, 7, tz="UTC"),
    schedule=None,          # manual, one-time
    catchup=False,
    tags=["de300", "hw4"],
) as dag:
    PythonOperator(
        task_id="generate_movie_embeddings",
        python_callable=generate_movie_embeddings,
    )
