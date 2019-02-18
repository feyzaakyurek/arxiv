

import urllib.request
import feedparser
from itertools import combinations
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys

class Query:

    def __init__(self):
        self.author_df = []
        self.collab_df = []

    def read_authors(self, feed, al):
        for entry in feed.entries:
            for name in (author.name for author in entry.authors):
                # maybe consider case insensitive comparing
                if name not in map(lambda t: t[1], al):
                    al.append((len(al),name))
        return al


    def read_collabs(self, feed, author_list, cl):
        collab_list = []
        for entry in feed.entries:
            title = entry.title
            arxiv_id = entry.id.split('/abs/')[-1]
            authors = (author.name for author in entry.authors)
            for a1,a2 in combinations(authors,2):
                collab_list.append((self.find_id(a1,author_list),self.find_id(a2,author_list),arxiv_id,title))
        return cl + collab_list


    def find_id(self, a, author_list):
        return [y[1] for y in author_list].index(a)


    def create_df(self, l, schema):
        rdd = sc.parallelize(l)
        return spark.createDataFrame(rdd,schema)

    def send_query(self, search_query, start, max_results = -1):
        base_url = 'http://export.arxiv.org/api/query?'
        query = 'search_query=%s&start=%i%s' % (search_query,
                                              start,
                                              "" if max_results == -1 else ("&max_results=%i"% max_results))
        # perform a GET request using the base_url and query
        response = urllib.request.urlopen(base_url+query).read()

        # parse the response using feedparser
        feed = feedparser.parse(response)

        return feed

    # def query(search_query, al, cl, start = 0, max_results = 2000, verbose = False):
    #     ### Querying Arxiv API
    #
    #     feed = send_query(search_query, start, max_results)
    #     print("Search q %s, start: %i, max_results: %i" % (search_query, start, max_results))
    #     count = len(feed.entries)
    #     if verbose:
    #         print('Feed last updated: %s' % feed.feed.updated)
    #         print('Total results for this query: %s' % feed.feed.opensearch_totalresults)
    #         print('Max results for this query: %s' % count)
    #
    #
    #     # Create a data frame for unique authors <AuthorID, AuthorName>
    #     author_list = read_authors(feed, al)
    #     collab_list = read_collabs(feed, author_list, cl)
    #
    #     return (author_list, collab_list, count, int(feed.feed.opensearch_totalresults))


    def query_all(self, search_query = "all", request = -1, batch = 1000):
        """ Query Arxiv and store parquet files

        This method takes in the search_query string, maximum
        number of requests and the size of each query to arxiv.
        The maximum batch size (query size) cannot be larger
        than 2000 according to Arxiv API.

        Keyword Arguments:
        search_query -- query string
        request -- requested number of papers
        batch -- size of each single query to Arxiv API.
        """

        # send to Arxiv to learn total number of papers complying the filter
        trial_feed = self.send_query(search_query, 0, 1)
        feedtotal = int(trial_feed.feed.opensearch_totalresults)
        print('Total results for this query: %i' % feedtotal)

        # create lists which will be converted to parquet files
        al = []
        cl = []

        # if request is not provided, set req to feedtotal
        req = feedtotal if request == -1 else min(request,feedtotal)
        t = batch
        totalresults = 0

        # send queries until req is fulfilled
        while totalresults < req: #(t == batch and totalresults < req) or (request == -1 and totalresults < feedtotal):
            feed = self.send_query(search_query, totalresults, batch)
            al = self.read_authors(feed, al)
            cl = self.read_collabs(feed, al, cl)
            t = len(feed.entries)
            # al, cl, t, _ = query(search_query, al, cl, start = totalresults, max_results = batch, verbose = False)
            # al = al + a
            # cl = cl + c
            totalresults = totalresults + t
            print("Query returned: ", t)
            print("Total results:%i" % totalresults)
            # if t == 0: t = batch #potential bug

        # create a dataframe containing author ids and names
        schema_a = StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True)
            ])
        self.author_df = self.create_df(al, schema_a)

        # create a dataframe containing collaborations
        schema_c = StructType([
                StructField("src", IntegerType(), True),
                StructField("dest", IntegerType(), True),
                StructField("arxiv", StringType(), True),
                StructField("title", StringType(), True)
            ])
        self.collab_df = self.create_df(cl, schema_c)

        # print results to parquet
        print("Query completed, length of unique authors: ", self.author_df.count() )
        print("Length of collabs: ", self.collab_df.count() )
        self.author_df.write.mode('overwrite') \
                .parquet("Data/authors-%s-total%i.parquet" % (search_query.replace(":",""), totalresults))
        self.collab_df.write.mode('overwrite') \
                .parquet("Data/collab-%s-total%i.parquet" % (search_query.replace(":",""), totalresults))
        print("Parquet written.")


# if __name__ == '__main__':

q = Query()

spark = SparkSession.builder \
        .master("local") \
        .appName("Word Count") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

sc = spark.sparkContext
if len(sys.argv) == 1:
    q.query_all(request = 2000)
elif len(sys.argv) == 2:
    q.query_all(search_query = sys.argv[1])
elif len(sys.argv) == 3:
    # print(sys.argv[1])
    q.query_all(search_query = sys.argv[1], request = int(sys.argv[2]))
else:
    print("Arguments invalid: ", sys.argv)

spark.stop()
# python query.py "cat:stat.ML+OR+cat:stat.AP+OR+cat:stat.CO+OR+cat:stat.ME+OR+cat:stat.OT+OR+cat:stat.TH" 200
