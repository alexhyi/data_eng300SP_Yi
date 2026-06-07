import io
import json
import logging
import pendulum

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator

BUCKET = "de300-airflow-yi-barnett"
RAW_PREFIX = "ml-1m"
EMB_KEY = "embeddings/movie_embeddings.parquet"
STATE_KEY = "state/accumulated_ratings.parquet"   
REC_PREFIX = "recommendations"
ITER_VAR = "recommend_iter"                      
SEED = 42

PARTITIONS = [
    ("part1", None,        964483200),   # ... <= 08/03/2000 boundary
    ("part2", 964483200,   972950400),   # 08/04/2000 - 10/31/2000
    ("part3", 972950400,   975196800),   # 11/01/2000 - 11/26/2000
    ("part4", 975196800,   None),        # 11/26/2000 and later
]


log = logging.getLogger(__name__)
default_args = {"owner": "de300_group", "retries": 1}


def _s3():
    import boto3
    return boto3.client("s3")


def _read_parquet(client, key):
    import pandas as pd
    obj = client.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def _write_parquet(client, df, key):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    client.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())


# 1 
def load_partition(**context):
    import pandas as pd
    client = _s3()

    idx = int(Variable.get(ITER_VAR, default_var=0))
    if idx >= len(PARTITIONS):
        raise AirflowSkipException(
            f"All {len(PARTITIONS)} iterations already completed; skipping extra run."
        )
    name, lo, hi = PARTITIONS[idx]

    obj = client.get_object(Bucket=BUCKET, Key=f"{RAW_PREFIX}/ratings.dat")
    ratings = pd.read_csv(
        io.BytesIO(obj["Body"].read()),
        sep="::", engine="python", encoding="latin-1",
        names=["UserID", "MovieID", "Rating", "Timestamp"],
    )
    if lo is not None:
        ratings = ratings[ratings["Timestamp"] > lo]
    if hi is not None:
        ratings = ratings[ratings["Timestamp"] <= hi]

    key = f"staging/iter={idx}/partition.parquet"
    _write_parquet(client, ratings, key)
    log.info("Iteration %d (%s): %d new ratings -> %s", idx, name, len(ratings), key)
    context["ti"].xcom_push(key="iter", value=idx)


# 2 
def handle_new_users(**context):
    client = _s3()
    idx = context["ti"].xcom_pull(key="iter")
    new = _read_parquet(client, f"staging/iter={idx}/partition.parquet")

    try:
        prev = _read_parquet(client, STATE_KEY)
        seen = set(prev["UserID"].unique())
    except Exception:
        seen = set()

    new_users = sorted(set(new["UserID"].unique()) - seen)
    log.info("Iteration %d: %d newly-arrived users", idx, len(new_users))
    context["ti"].xcom_push(key="new_users", value=[int(u) for u in new_users])


# 3 
def sample_users(**context):
    import pandas as pd
    client = _s3()
    idx = context["ti"].xcom_pull(key="iter")
    new = _read_parquet(client, f"staging/iter={idx}/partition.parquet")

    users = pd.Series(new["UserID"].unique())
    sample = users.sample(frac=0.30, random_state=SEED).tolist()
    log.info("Iteration %d: sampled %d/%d users (30%%)", idx, len(sample), len(users))
    context["ti"].xcom_push(key="sampled_users", value=[int(u) for u in sample])


# 4 
def compute_user_embeddings(**context):
    import pandas as pd
    client = _s3()
    idx = context["ti"].xcom_pull(key="iter")
    sampled = set(context["ti"].xcom_pull(key="sampled_users"))

    new = _read_parquet(client, f"staging/iter={idx}/partition.parquet")
    movie_emb = _read_parquet(client, EMB_KEY).set_index("MovieID")

    sub = new[new["UserID"].isin(sampled)]
    rows = []
    for uid, grp in sub.groupby("UserID"):
        vecs = movie_emb.reindex(grp["MovieID"]).dropna()
        if len(vecs):
            rows.append([uid] + list(vecs.mean(axis=0).values))
    cols = ["UserID"] + list(movie_emb.columns)
    user_emb = pd.DataFrame(rows, columns=cols)

    _write_parquet(client, user_emb, f"staging/iter={idx}/user_embeddings.parquet")
    log.info("Iteration %d: computed %d user embeddings", idx, len(user_emb))


# 5 
def combine_with_previous(**context):
    import pandas as pd
    client = _s3()
    idx = context["ti"].xcom_pull(key="iter")
    new = _read_parquet(client, f"staging/iter={idx}/partition.parquet")

    try:
        prev = _read_parquet(client, STATE_KEY)
        combined = pd.concat([prev, new], ignore_index=True)
    except Exception:
        combined = new
    combined = combined.drop_duplicates(subset=["UserID", "MovieID", "Timestamp"])
    _write_parquet(client, combined, STATE_KEY)
    log.info("Iteration %d: accumulated state now %d ratings", idx, len(combined))


# 6 
def generate_recommendations(**context):
    import numpy as np
    client = _s3()
    idx = context["ti"].xcom_pull(key="iter")

    history = _read_parquet(client, STATE_KEY)
    movie_emb = _read_parquet(client, EMB_KEY).set_index("MovieID")
    M = movie_emb.values
    Mn = M / (np.linalg.norm(M, axis=1, keepdims=True) + 1e-9)
    movie_ids = movie_emb.index.values

    def top5(uvec, exclude):
        un = uvec / (np.linalg.norm(uvec) + 1e-9)
        scores = Mn @ un
        order = np.argsort(-scores)
        out = [int(movie_ids[i]) for i in order if int(movie_ids[i]) not in exclude]
        return out[:5]

    recs = []

    # Cold
    pop = history["MovieID"].value_counts().head(5).index.tolist()
    recs.append({
        "User_Type": "cold",
        "User_ID": None,
        "Last_Interaction_Time": None,
        "num_ratings_observed": 0,
        "recommended_movies": [int(m) for m in pop],
    })

    # Top
    counts = history.groupby("UserID").size()
    threshold = counts.quantile(0.95)
    top_pool = counts[counts >= threshold].index.tolist()
    rng = np.random.default_rng(SEED + idx)
    top_uid = int(rng.choice(top_pool))
    u_hist = history[history["UserID"] == top_uid]
    seen_movies = set(u_hist["MovieID"].tolist())
    uvec = movie_emb.reindex(u_hist["MovieID"]).dropna().mean(axis=0).values
    recs.append({
        "User_Type": "top",
        "User_ID": top_uid,
        "Last_Interaction_Time": int(u_hist["Timestamp"].max()),
        "num_ratings_observed": int(len(u_hist)),
        "recommended_movies": top5(uvec, seen_movies),
    })

    context["ti"].xcom_push(key="recs", value=recs)


# 7 
def write_outputs(**context):
    client = _s3()
    idx = context["ti"].xcom_pull(key="iter")
    recs = context["ti"].xcom_pull(key="recs")
    ds = context["ds"]

    # Non-overwriting name: partitioned by iteration index (and logical date).
    key = f"{REC_PREFIX}/dt={ds}/iter={idx}/recommendations.json"
    body = json.dumps({"iteration": idx, "ds": ds, "results": recs}, indent=2)
    client.put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"))
    log.info("Wrote recommendations to s3://%s/%s", BUCKET, key)

    # Advance the counter only after a fully successful run.
    Variable.set(ITER_VAR, idx + 1)
    log.info("Advanced %s -> %d", ITER_VAR, idx + 1)


with DAG(
    dag_id="recommend_pipeline",
    default_args=default_args,
    description="Iterative BERT recommendation pipeline (4 runs).",
    start_date=pendulum.datetime(2026, 6, 7, tz="UTC"),
    schedule="0 */10 * * *",   
    catchup=False,
    max_active_runs=1,
    tags=["de300", "hw4"],
) as dag:
    t1 = PythonOperator(task_id="load_partition", python_callable=load_partition)
    t2 = PythonOperator(task_id="handle_new_users", python_callable=handle_new_users)
    t3 = PythonOperator(task_id="sample_users", python_callable=sample_users)
    t4 = PythonOperator(task_id="compute_user_embeddings", python_callable=compute_user_embeddings)
    t5 = PythonOperator(task_id="combine_with_previous", python_callable=combine_with_previous)
    t6 = PythonOperator(task_id="generate_recommendations", python_callable=generate_recommendations)
    t7 = PythonOperator(task_id="write_outputs", python_callable=write_outputs)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
