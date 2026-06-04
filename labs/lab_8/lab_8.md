# Lab 8

**Name:** Alex

## Questions

**1. What is the partition key of `plays_by_user`?**

`user_id`. It is the first element of the primary key `(user_id, played_at, song_id)`.

**2. What are the clustering columns of `plays_by_user`?**

`played_at` and `song_id`

**3. Why did we create both `plays_by_user` and `plays_by_song` instead of using one table?**

Cassandra doesn't join and queries must be served by the partition key, so we design one table per query pattern. Query 1 needs `user_id` as the partition key, while Query 2 needs `song_id` as the partition key (a single table can only have one partition key). We therefore have to store the same events twice.

**4. What happens if you try to query `plays_by_user` by `song_id` only?**

Cassandra rejects it with an `InvalidRequest` error because `song_id` is a clustering column, not the partition key.

**5. Why is data duplication common in Cassandra?**

Again, there are no joins in cassandra so you need to duplicate depending on your query.

**6 and 7**

The screenshot with the two query outputs and the two csv files are in the same folder
within the GitHub repo. 
