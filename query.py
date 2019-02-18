

import urllib.request
import feedparser
from itertools import combinations
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys



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

    feed = query_arxiv(search_query, start, max_results)
    print("Search q %s, start: %i, max_results: %i" % (search_query, start, max_results))
    count = len(feed.entries)
    if verbose:
        print('Feed last updated: %s' % feed.feed.updated)
        print('Total results for this query: %s' % feed.feed.opensearch_totalresults)
        print('Max results for this query: %s' % count)


    # Create a data frame for unique authors <AuthorID, AuthorName>
    author_list = read_authors(feed)
    collab_list = read_collabs(feed, author_list)

    return (author_list, collab_list, count, feed.feed.opensearch_totalresults)


def query_all(search_query = "all", request = -1, batch = 1000):
    al, cl, t, feedtotal = query(search_query, start = 0, max_results = batch, verbose = True)
    totalresults = t
    print("Query returned: ", t)
    print("Total Results: ", totalresults)
    req = feedtotal if request == -1 else request

    while t == batch and totalresults < req:
        a, c, t, _ = query(search_query, start = totalresults, max_results = batch, verbose = False)
        al = al + a
        cl = cl + c
        totalresults = totalresults + t
        print("Query returned: ", t)
        print("Total results:%i" % totalresults)

    schema_a = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
    author_df = create_df(al, schema_a)

    schema_c = StructType([
            StructField("src", IntegerType(), True),
            StructField("dest", IntegerType(), True),
            StructField("arxiv", StringType(), True),
            StructField("title", StringType(), True)
        ])
    collab_df = create_df(cl, schema_c)

    print("Query completed, length of unique authors: ", author_df.count() )
    print("Length of collabs: ", collab_df.count() )
    author_df.write.mode('overwrite').parquet("Data/authors-%s-total%i.parquet" % (search_query.replace(":",""), totalresults))
    collab_df.write.mode('overwrite').parquet("Data/collab-%s-total%i.parquet" % (search_query.replace(":",""), totalresults))
    print("Parquet written.")

spark = SparkSession.builder \
        .master("local") \
        .appName("Word Count") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

sc = spark.sparkContext
if len(sys.argv) == 1:
    query_all(request = 2000)
elif len(sys.argv) == 2:
    query_all(search_query = sys.argv[1])
elif len(sys.argv) == 3:
    query_all(search_query = sys.argv[1], request = int(sys.argv[2]))
else:
    print("Arguments invalid: ", sys.argv)

spark.stop()
# python query.py "cat:stat.ML+OR+cat:stat.AP+OR+cat:stat.CO+OR+cat:stat.ME+OR+cat:stat.OT+OR+cat:stat.TH" 200
