class Article:
    """
        Object that assists in storing the articles for easy access
    """
    # Personal Note: Is this really that useful?
    def __init__(self, article):
        self._source_result = article['source']['name']
        self._author_result = article['author']
        self._title_result = article['title']
        self._description_result = article['description']
        self._url_result = article['url']
        self._image_result = article['urlToImage']
        self._timestamp_result = article['publishedAt']
        self._content_result = article['content']

        self._dicArticle = {
            'Title' : article['title'],
            'Source' : article['source']['name'],
            'Author' : article['author'],
            'Description' : article['description'],
            'Url' : article['url'],
            'Image' : article['urlToImage'],
            'Timestamp' : article['publishedAt'],
            'Content' : article['content']
        }
    
    def source(self):
        return self._source_result

    def author(self):
        return self._author_result

    def title(self):
        return self._title_result

    def description(self):
        return self._description_result
        
    def url(self):
        return self._url_result

    def image(self):
        return self._image_result

    def timestamp(self):
        return self._timestamp_result

    def content(self):
        return self._content_result

    def dicArticle(self):
        return self._dicArticle
    