To run this code, you have to ensure that the ml-1m dataset is unzipped in the s3 bucket.

Then, both dags must be in /dags folder

Run the movie_embeddings dag first to get the embeddings

Then, run the recommend_pipeline 4 times to get each time period
