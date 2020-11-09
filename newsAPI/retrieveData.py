
from newsapi import NewsApiClient
#from articleStruct import Article
from newResult import NewsResult
from datetime import datetime, timedelta
import sys
import pprint as pp

# Import the secret keys required
sys.path.insert(1, r'C:\Users\7h3un\Documents\pythonprojects\py-script\sercrets')
from secrets import key

class NewsQuery(object):

    def __init__(self, minutesHistory):
        '''Initial constructor that queries the API for data of a specified amount of time'''
        # Populate Sources
        self._sources_text = self.getSources()
        self._results = None

        # Calculate the time now and X minutes ago
        self._now = datetime.now()
        elapse_time = timedelta(minutes=minutesHistory)
        self._past = self._now - elapse_time

        #create API
        self._newsapi = NewsApiClient(api_key=key('newsAPI'))

        self._run()

        
    def _run(self):
         '''Retrieve the Results from the API, This will soon be a WebScraper looking for URLS'''
         results = self._newsapi.get_everything(sources=self._sources_text, from_param= self._format_time(self._now.isoformat()), to=self._format_time(self._past.isoformat()), language='en')

         self._results = NewsResult(results)

    def _format_time(self, time):
        # TODO: move the time calculation funtionality here to that any changes don't affect the constructor
        '''This is used to remove the micro seconds from the time format. It's cleaner'''
        return time[:-7]

    def getSources(self):
        '''Static list of supported sources'''

        sources = [
        'associated-press',
        'bloomberg',
        'Business Insider',
        'cnn',
        'new-york-magazine']

        sources_text = ','.join(sources)

        return sources_text

    def getResults(self, values='Title, Url, Timestamp, Source, Author, Image, Description, Content'):
        ''' Allows the caller to specify what fields they would like to return'''
        # TODO: Add error handling and input validation
        # Extract the values we are looking for 
        fields = values.split(', ')
        entry = {}
        payload = []

        for article in self._results.articles():
            article_dictionary = article.dicArticle()
            for key in article_dictionary.keys():
                if key in fields:
                    entry[key] = article_dictionary[key]
            payload.append(entry)
            entry = {}
            
        return payload


if __name__ == "__main__":
    top_headlines = NewsQuery(15)
    results = top_headlines.getResults("Url")
    pp.pprint(results)
    print(top_headlines.getSources())
