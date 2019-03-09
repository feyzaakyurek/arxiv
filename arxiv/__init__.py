from .query import Query
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import queue

spark = SparkSession.builder \
        .master("local") \
        .appName("Word Count") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

sc = spark.sparkContext

path = "catstat.ML+OR+catstat.AP+OR+catstat.CO+OR+catstat.ME+OR+catstat.OT+OR+catstat.TH-total49904"

pa = "Data/authors-" + path + ".parquet"
pc = "Data/collab-" + path + ".parquet"

author_df = spark.read.parquet(pa)
collab_df = spark.read.parquet(pc)

def dist(a1, a2, depth_max = 3):

    r = author_df.where(author_df.name == a1).select("id")
    author1 = r.rdd.map(lambda x: x.id).first()

    r = author_df.where(author_df.name == a2).select("id")
    author2 = r.rdd.map(lambda x: x.id).first()

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
        # print("AuthorID:", a)
        if a == author2:
            break

        if d >= depth_max:
            print("Depth of %i is reached." % d)
            break

        # "src" in collab item is equal to author1, look for the authors in "dest"
        df_dest = collab_df.filter(collab_df.src == a).select(collab_df.columns[1])
        for i in [int(row.dest) for row in df_dest.collect()]:
            # print("Next author: %i" % i)
            if i not in parents: #if already visited, don't add to queue
                fifo.put(i); depth.put(d + 1)
                parents[i] = a

        # "dest" in collab item is equal to author1, look for the authors in "src"
        df_src = collab_df.filter(collab_df.dest == a).select(collab_df.columns[0])
        for i in [int(row.src) for row in df_src.collect()]:
            # print("Next author: %i" % i)
            if i not in parents:
                fifo.put(i); depth.put(d + 1)
                parents[i] = a

    # Calculate the depth
    # dist = 0
    ancestry = [a]
    while parents[a] > 0:
        # dist = dist + 1
        a = parents[a]
        ancestry = [a] + ancestry

    for i in range(len(ancestry)-1):
        id1 = ancestry[i]
        id2 = ancestry[i+1]
        df1 = collab_df.filter((collab_df.src == id1) | (collab_df.dest == id1))
        df2 = df1.filter((collab_df.src == id2) | (collab_df.dest == id2))
        # df2.show()
        df3 = df2.join(author_df, df2.src == author_df.id, 'left')\
        .withColumnRenamed('name','Author_1')\
        .select('src','Author_1','dest', 'arxiv', 'title')
        # df3.show()
        df4 = df3.join(author_df, df3.dest == author_df.id, 'left')\
        .withColumnRenamed('name', "Author_2")\
        .withColumnRenamed('title', "Paper Title")\
        .withColumnRenamed('arxiv', "Arxiv ID")\
        .select("Author_1", "Author_2", "Paper Title","Arxiv ID")


        df4.show()
        # r = author_df.where(author_df.id == i).select("name")
        # author1 = df3.rdd.map(lambda x: x.id).first()
        # df3.first().collect()
    # TODO: return names instead of ids
    # return (d, ancestry)
    print("Distance: ", d)
