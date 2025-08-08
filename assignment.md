# Assignment

## Brief

Write the Python codes for the following questions.

## Instructions

Paste the answer as Python in the answer code section below each question.

### Question 1

Question: From the `movies` collection, return the documents with the `plot` that starts with `"war"` in acending order of released date, print only title, plot and released fields. Limit the result to 5.

Answer:

```python
for movie in movies.find({"plot": {"$regex": "^war", "$options": "i"}}) \
                   .sort("released", pymongo.ASCENDING) \
                   .limit(5):
    print({
        "title": movie.get("title"),
        "plot": movie.get("plot"),
        "released": movie.get("released")
    })
```

### Question 2

Question: Group by `rated` and count the number of movies in each.

Answer:

```python
#Group by `rated` and count the number of movies in each
pipeline = [
    {
        "$group": {
            "_id": "$rated",
            "count": {"$sum": 1}
        }
    }
]

# Execute the aggregation pipeline
results = movies.aggregate(pipeline)

# Print the results
for result in results:
    print(f"Rating: {result['_id']}, Count: {result['count']}")
```

### Question 3

Question: Count the number of movies with 3 comments or more.

Answer:

```python
# Count the number of movies with 3 comments or more (optimized pipeline)
# Ensure index exists for fast lookup
index_name = db.comments.create_index("movie_id")

pipeline = [
    {"$lookup": {
        "from": "comments",
        "let": {"movie_id": "$_id"},
        "pipeline": [
            { "$match": { "$expr": { "$eq": ["$movie_id", "$$movie_id"] } } }
        ],
        "as": "comments"
    }},
    {"$match": {
        "$expr": {
            "$gte": [{ "$size": "$comments" }, 3]
        }
    }},
    {"$count": "num_movies"}
]

# Execute the aggregation pipeline
results = movies.aggregate(pipeline)

# Print the results
for result in results:
    print(f"Number of movies with 3 comments or more: {result['num_movies']}")
```

## Submission

- Submit the URL of the GitHub Repository that contains your work to NTU black board.
- Should you reference the work of your classmate(s) or online resources, give them credit by adding either the name of your classmate or URL.
