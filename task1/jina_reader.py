import requests
import os
class JinaReader():
    def __init__(self):
        api_key = os.getenv("JINA_API")
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def read(self, url):
        url = f"https://r.jina.ai/{url}"
        print(url, self.headers)
        return requests.get(url, self.headers).text
        

    def search(self, query):
        url = f"https://s.jina.ai/ {query}"
        return requests.get(url, self.headers).text
if __name__ == '__main__':         
    r = JinaReader()
    papers = []
    papers.append("https://arxiv.org/pdf/1512.03385")
    papers.append("https://arxiv.org/pdf/1706.03762")
    papers.append("https://arxiv.org/pdf/2103.00020")
    papers.append("https://arxiv.org/pdf/2005.14165")
    papers.append("https://arxiv.org/abs/2201.11903k")

    doc_text = ""
    for paper in papers:
        text = r.read(paper)
        doc_text += text

    with open("ml_corpus.txt", "w") as fw:
        fw.write(doc_text)
