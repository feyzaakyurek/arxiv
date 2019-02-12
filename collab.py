
from pyspark.sql import Row
from pyspark.sql.types import *
import queue

## Reading Data from Disk and Calculate Collab Distance

### Read from Disk
def read_arxiv():
    author_df = spark.read.parquet("Data/authors-cat:stat.ML.parquet")
    collab_df = spark.read.parquet("Data/collab-cat:stat.ML.parquet")


def collab_dist(author1, author2, depth_max = 10):

    # BFS
    fifo = queue.Queue()
    fifo.put(author1)

    # To track depth to stop search at max depth
    depth = queue.Queue()
    depth.put(0)

    # To find depth and the path backwards
    parents = {author1 : -1}

    while not fifo.empty():
        a = fifo.get() ; d = depth.get()
        print("AuthorID:", a)
        if a == author2:
            break

        if d >= depth_max:
            print("Max depth of %i is reached." % d)
            break

        # "src" in collab item is equal to author1, look for the authors in "dest"
        df_dest = collab_df.filter(collab_df.src == a).select(collab_df.columns[1])
        for i in [int(row.dest) for row in df_dest.collect()]:
            print("Next author: %i" % i)
            if i not in parents: #if already visited, don't add the queue
                fifo.put(i); depth.put(d + 1)
                parents[i] = a

        # "dest" in collab item is equal to author1, look for the authors in "src"
        df_src = collab_df.filter(collab_df.dest == a).select(collab_df.columns[0])
        for i in [int(row.src) for row in df_src.collect()]:
            print("Next author: %i" % i)
            if i not in parents:
                fifo.put(i); depth.put(d + 1)
                parents[i] = a

    # Calculate the depth.
    dist = 0
    while parents[a] > 0:
        dist = dist + 1
        a = parents[a]

    return (dist, d, parents)
