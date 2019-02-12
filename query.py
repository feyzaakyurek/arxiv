

import urllib.request
import feedparser
from itertools import combinations
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def query_arxiv(search_query, start, max_results = -1):
    base_url = 'http://export.arxiv.org/api/query?'
    query = 'search_query=%s&start=%i%s' % (search_query,
                                          start,
                                          "" if max_results == -1 else ("&max_results=%i"% max_results))
    # perform a GET request using the base_url and query
    response = urllib.request.urlopen(base_url+query).read()

    # parse the response using feedparser
    feed = feedparser.parse(response)

    return feed


def read_authors(feed):
    author_list = []
    for entry in feed.entries:
        for name in (author.name for author in entry.authors):
            # maybe consider case insensitive comparing
            if name not in map(lambda t: t[1], author_list):
                author_list.append((len(author_list),name))
    return author_list


def read_collabs(feed, author_list):
    collab_list = []
    for entry in feed.entries:
        title = entry.title
        arxiv_id = entry.id.split('/abs/')[-1]
        authors = (author.name for author in entry.authors)
        for a1,a2 in combinations(authors,2):
            collab_list.append((find_id(a1,author_list),find_id(a2,author_list),arxiv_id,title))
    return collab_list


def find_id(a, author_list):
    return [y[1] for y in author_list].index(a)


def create_df(l, schema):
    rdd = sc.parallelize(l)
    return spark.createDataFrame(rdd,schema)

def query(search_query = "all", start = 0, max_results = 2000, verbose = False):
    ### Querying Arxiv API

    # Query parameters
    # search_query = 'au:%22daphne+koller%22'
    # search_query = 'cat:stat.ML'
    # start = 0
    # max_results = 2000

    # Querying the Arxiv API
    feed = query_arxiv(search_query, start, max_results)
    if verbose:
        print('Feed last updated: %s' % feed.feed.updated)
        print('Total results for this query: %s' % feed.feed.opensearch_totalresults)
        print('Max results for this query: %s' % len(feed.entries))


    # Create a data frame for unique authors <AuthorID, AuthorName>
    author_list = read_authors(feed)
    schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
    author_df = create_df(author_list, schema)
    author_df.filter(author_df.id  < 10).collect()
    # author_df.collect()

    # author_df.filter(author_df.name == "Larry Wasserman").collect()
    # Create a data frame for collaborations <Author1ID, Author2ID, PaperArxivID, PaperTitle>
    collab_list = read_collabs(feed, author_list)
    schema = StructType([
            StructField("src", IntegerType(), True),
            StructField("dest", IntegerType(), True),
            StructField("arxiv", StringType(), True),
            StructField("title", StringType(), True)
        ])
    collab_df = create_df(collab_list, schema)
    # collab_df.collect()
    return (author_df, collab_df, feed.feed.opensearch_totalresults)
    ### Save Author  and Collab Dataframes to Disk

def query_all(max_results = 20000, batch = 100):
    search_query = "all"
    author_df, collab_df, t = query(search_query)
    totalresults = t

    while t == batch and totalresults < max_results:
        a, c, t = query(search_query, start = totalresults, max_results = batch, verbose = True)
        author_df = author_df.union(a)
        collab_df = collab_df.union(c)
        totalresults = totalresults + t
        print("Total results:%i" % totalresults)

    author_df.write.mode('overwrite').parquet("Data/authors-%s.parquet" % search_query)
    collab_df.write.mode('overwrite').parquet("Data/collab-%s.parquet" % search_query)

spark = SparkSession.builder \
        .master("local") \
        .appName("Word Count") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

sc = spark.sparkContext
query_all(max_results = 400, batch = 100)
spark.stop()
