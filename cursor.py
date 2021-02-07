# %%
"""
<img src="https://s3.amazonaws.com/edu-static.mongodb.com/lessons/M220/notebook_assets/screen_align.png" style="margin: 0 auto;">

"""

# %%
"""
<h1 style="text-align: center; font-size=58px;">Cursor Methods and Aggregation Equivalents</h1>
"""

# %%
"""
In this lesson we're going to discuss methods we can call against Pymongo cursors, and the aggregation stages that would perform the same tasks in a pipeline.
"""

# %%
"""
<h2 style="text-align: center; font-size=58px;">Limiting</h2>
"""

# %%
import pymongo
from bson.json_util import dumps
uri = "<your_atlas_uri>"
client = pymongo.MongoClient(uri)
mflix = client.sample_mflix
movies = mflix.movies

# %%
"""
Here's (point) a collection object for the `movies` collection.
"""

# %%
limited_cursor = movies.find(
    { "directors": "Sam Raimi" },
    { "_id": 0, "title": 1, "cast": 1 } 
).limit(2)

print(dumps(limited_cursor, indent=2))

# %%
"""
So this is a find query with a predicate (point) and a projection (point). And the find() method is always gonna return a cursor to us. But before assigning that cursor to a variable, we've transformed it with the limit() method, to make sure no more than 2 documents are returned by this cursor.

(run command)

And we can see we only got two (point) documents back.
"""

# %%
pipeline = [
    { "$match": { "directors": "Sam Raimi" } },
    { "$project": { "_id": 0, "title": 1, "cast": 1 } },
    { "$limit": 2 }
]

limited_aggregation = movies.aggregate( pipeline )

print(dumps(limited_aggregation, indent=2))

# %%
"""
Now this is the equivalent operation with the aggregation framework. Instead of tacking a .limit() to the end of the cursor, we add $limit as a stage in our pipeline.

(enter command)

And it's the same output. And these (point to `$match` and `$project`) aggregation stages represent the query predicate and the projection from when we were using the query language.
"""

# %%
"""
<h2 style="text-align: center; font-size=58px;">Sorting</h2>
"""

# %%
from pymongo import DESCENDING, ASCENDING

sorted_cursor = movies.find(
    { "directors": "Sam Raimi" },
    { "_id": 0, "year": 1, "title": 1, "cast": 1 } 
).sort("year", ASCENDING)

print(dumps(sorted_cursor, indent=2))

# %%
"""
This is an example of the `sort()` (point) cursor method. `sort()` takes two parameters, the key we're sorting on and the sorting order. In this example we're sorting on year (point), in increasing (point) order.

ASCENDING and DESCENDING are values from the pymongo library to specify sort direction, but they're really just the integers 1 and -1.

(enter command)

And we can see that the movies were returned to us in order of the year they were made.
"""

# %%
pipeline = [
    { "$match": { "directors": "Sam Raimi" } },
    { "$project": { "_id": 0, "year": 1, "title": 1, "cast": 1 } },
    { "$sort": { "year": ASCENDING } }
]

sorted_aggregation = movies.aggregate( pipeline )

print(dumps(sorted_aggregation, indent=2))

# %%
"""
And this is the equivalent pipeline, with a sort (point) stage that corresponds to a dictionary, giving the sort (point) field, and the direction (point) of the sort.

(enter command)

And the agg framework was able to sort by year here.
"""

# %%
sorted_cursor = movies.find(
    { "cast": "Tom Hanks" },
    { "_id": 0, "year": 1, "title": 1, "cast": 1 }
).sort([("year", ASCENDING), ("title", ASCENDING)])

print(dumps(sorted_cursor, indent=2))

# %%
"""
So just a special case to note here, sorting on multiple keys in the cursor method is gonna look a little different.

When sorting on one key, the `sort()` method takes two arguments, the key and the sort order.

When sorting on two or more keys, the `sort()` method takes a single argument, an array of tuples. And each tuple has a key and a sort order.

(enter command)

And we can see that after sorting on year, the cursor sorted the movie titles alphabetically.
"""

# %%
pipeline = [
    { "$match": { "cast": "Tom Hanks" } },
    { "$project": { "_id": 0, "year": 1, "title": 1, "cast": 1 } },
    { "$sort": { "year": ASCENDING, "title": ASCENDING } }
]

sorted_aggregation = movies.aggregate( pipeline )

print(dumps(sorted_aggregation, indent=2))

# %%
"""
<h2 style="text-align: center; font-size=58px;">Skipping</h2>
"""

# %%
pipeline = [
    { "$match": { "directors": "Sam Raimi" } },
    { "$project": { "_id": 0, "title": 1, "cast": 1 } },
    { "$count": "num_movies" }
]

sorted_aggregation = movies.aggregate( pipeline )

print(dumps(sorted_aggregation, indent=2))

# %%
"""
(enter command)

So we know from counting the documents in this aggregation, that if we don't specify anything else, we're getting 15 (point) documents returned to us.

Note that the cursor method `count()` that counts documents in a cursor has been deprecated. So if you want to know how many documents are returned by a query, you should use the `$count` aggregation stage.
"""

# %%
skipped_cursor = movies.find(
    { "directors": "Sam Raimi" },
    { "_id": 0, "title": 1, "cast": 1 } 
).skip(14)

print(dumps(skipped_cursor, indent=2))

# %%
"""
The `skip()` method allows us to skip documents in a collection, so only documents we did not skip appear in the cursor. Because we only have 15 documents, skipping 14 of them should only leave us with 1.

(enter command)

And look at that, we've only got 1 document in our cursor. The issue is, we don't really know which documents we skipped over, because we haven't specified a sort key and really, we have no idea the order in which documents are stored in the cursor.
"""

# %%
skipped_sorted_cursor = movies.find(
    { "directors": "Sam Raimi" },
    { "_id": 0, "title": 1, "year": 1, "cast": 1 } 
).sort("year", ASCENDING).skip(10)

print(dumps(skipped_sorted_cursor, indent=2))

# %%
"""
So here we've sorted on year (point) and then skipped the first 14. Now we know that when we're skipping 10 documents, we're skipping the 10 oldest Sam Raimi movies in this collection.

(enter command)

And we only got 5 of those 15 documents back, because we skipped 10 of them.

These cursor methods are nice because we can tack them on a cursor in the order we want them applied. It even kinda makes our Python look like Javascript, with this `.sort()` and `.skip()`.
"""

# %%
pipeline = [
    { "$match": { "directors": "Sam Raimi" } },
    { "$project": { "_id": 0, "year": 1, "title": 1, "cast": 1 } },
    { "$sort": { "year": ASCENDING } },
    { "$skip": 10 }
]

sorted_skipped_aggregation = movies.aggregate( pipeline )

print(dumps(sorted_skipped_aggregation, indent=2))

# %%
"""
So here's an example of the same query in the aggregation framework. As you can see the `$skip` stage represents the `.skip()` from before.

(run command)

And it gives us the same output.

The `skip()` method is useful for paging results on a website, because we can sort the results chronologically, and then if we have 10 movies displayed on each page, the first page would have a skip value of 0, but then the second page would skip the first 10 movies, the third page would skip the first 20 movies, etc.
"""

# %%
"""
## Summary

* `.limit()` == `$limit`
* `.sort()` == `$sort`
* `.skip()` == `$skip`
"""

# %%
"""
So just to recap, in this lesson we covered some cursor methods and their aggregation equivalents. Remember that there won't always be a 1 to 1 mapping, because the aggregation framework can do a lot more than cursors can.

But these three methods exist as both aggregation stages and cursor methods.
"""