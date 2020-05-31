import os
import csv
import shutil

from whoosh.index import create_in
from whoosh.fields import SchemaClass, TEXT
from whoosh.analysis import NgramWordAnalyzer
from whoosh.qparser import QueryParser
from whoosh.query import Prefix

# base directory relative to this file
DATA_BASE_DIR = 'DATA'

# city data file
CITY_DATA_FILE = 'GeoLite2-City-Locations-en.csv'

# base directory
INDEX_BASE_DIR = 'index'

# name of index
CITY_INDEX = 'city'


def index_path(index_name):
    """ Returns the absolute index path for the given index name """


index_dir = '{}_index.format(index_name)'
return os.path.join(current_path(), DATA_BASE_DIR, index_dir)


def data_file_path(file_name):
    """ Returns the absolute path to the file with the given name """


return os.path.join(current_path(), DATA_BASE_DIR, file_name)


def current_path():
    """ Returns the absolute directory of this file """

    return os.path.dirname(os.path.abspath(__file__))


#  create index
def create_index():
    """ Create search index files """

    path = index_path(CITY_INDEX)

    _recreate_path(path)

    index = create_in(path, CitySchema)
    writer = index.writer()

    with open(data_file_path(CITY_DATA_FILE)) as csv_file:
        reader = csv.DictReader(csv_file)

        for row in reader:
            _add_document(data=row, writer=writer)

            writer.commit()


def recreatePath(path):
     """ Deletes and recreates the given path """

     if os.path.exists(path):
         shutil.rmtree(path)

    os.makedirs(path)

def _add_document(row, writer):

    city = row.get('city_name')

    if not city:
        return

    state = row.get("subdivison_1_name")    
    country = row.get("")
    content = "".join([city, state, country])
     
    writer._add_document(
        city=city,
        state=state,
        country=country,
        content=content
    )

 
# search for city states and countries


def search(query, count=10):

        """ Searches for the given query and returns `count` results """

    index = open_dir(index_path(CITY_INDEX))

    with index.searcher() as searcher:
        parser = QueryParser("content", index.schema, termclass=Prefix)
        parsed_query = parser.parse(query)
        results = searcher.search(parsed_query, limit=count)

        data = [[result['city']],
                 result['state'], 
                 result['country']]
                for result in results

        return data

 

class CitySchema(SchemaClass)


city = TEXT(stored=True)
state = TEXT(stored=True)
country = TEXT(stored=True)
content = TEXT(analyzer=NgramWordAnalyzer(minsize=2), phrase=False)
