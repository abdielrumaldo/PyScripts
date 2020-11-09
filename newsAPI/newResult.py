from articleStruct import Article

class NewsResult:
    """
        Object that assists in storing the result from articles
    """
    def __init__(self, result):
    
        self.status_result = result['status']
        # Check Status Code
        if 'ok' not in self.status_result:
            print(f"ERROR: There was an issue retrieving the result.\nStatus:{self.status_result}")
            return

        # Check of results
        self.number_result = result['totalResults']
        if self.number_result == 0:
            print("There are not results!")
            return

        self.articles_list = []
        # We need to reverse the list since we are retrieving the newest articles first
        result['articles'].reverse()
        for item in result['articles']:
            self.articles_list.append(Article(item))


    def status(self):
        return self.status_result

    def count(self):
        return self.number_result

    def articles(self):
        return self.articles_list
