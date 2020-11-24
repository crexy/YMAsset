from urllib import parse
from bs4 import BeautifulSoup
import requests
from konlpy.tag import Okt
from eunjeon import Mecab

from pymongo import MongoClient

# pip install konlpy 후
# nth-child 에서 "NotImplementedError: Only the following pseudo-classes are implemented: nth-of-type." 오류발생
# nth-child -> nth-of-type

class StockInfoCrawling:

    okt = Okt()
    mecab = Mecab()

    def crawlingData(self, stock_name, stock_code):
        # 네이버 news URL
        naver_news_url = parse.urlparse(f'https://search.naver.com/search.naver?&where=news&query={stock_name}&sm=tab_tmr&frm=mr&nso=so:r,p:all,a:all&sort=0')

        # 싱크풀 검색 URL
        thinkPool_search_url =f'http://www.thinkpool.com/itemanal/i/index.jsp?mcd=Q0&code={stock_code}&Gcode=001_002'

        naver_finance_url=f'https://finance.naver.com/item/main.nhn?code={stock_code}'
        response = requests.get(naver_finance_url)
        soup = BeautifulSoup(response.text, 'lxml')
        self.naver_finance_crawling(soup)

    def naver_finance_crawling(self, soup):
        #news = soup.select(f'div.section.new_bbs > div.sub_section.news_section li')
        news = soup.select(f'div.section.new_bbs > div.sub_section.news_section li')
        for li in news:
            a = li.select('a:nth-of-type(1)')
            #a = li.select('a')
            print(f'title:{a[0].text}, href:{a[0]["href"]}')
            self.naver_stock_news_article(a[0]['href'])
            break

    def naver_stock_news_article(self, href):
        url = f'https://finance.naver.com{href}'
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'lxml')

        title = soup.select('table.view > tbody > tr:nth-of-type(1) > th > strong')[0].text.strip()
        newspaper = soup.select('table.view > tbody > tr:nth-of-type(2) > th > span')[0].text.strip()
        postingDate = soup.select('table.view > tbody > tr:nth-of-type(2) > th > span > span')[0].text.strip()
        content = soup.select('#news_read')[0].text.strip()
        newspaper = newspaper.replace(postingDate,'')

        pos = content.find('▶')
        content = content[:pos]
        print(title)
        print(newspaper)
        print(postingDate)
        #print(set([n for n in self.han.phrases(content) if len(n) > 1]))
        #print([n for n in self.han.nouns(content) if len(n) > 1])
        #print([n for n in self.han.morphs(content) if len(n) > 1])
        #print([n for n in self.okt.phrases(content) if len(n) > 10])

        listPhr = self.mecab.nouns(content)
        print(set([n for n in listPhr if len(n) > 1]))
        #print(listPhr)
        '''
        listDel = [];
        listPhr = self.han.nouns(content)
        print(listPhr)
        for i, phr in enumerate(listPhr):
            if i < len(listPhr)-1:
                if listPhr[i+1].find(phr) != -1:
                    listDel.append(i)
        listDel.reverse()
        for i in listDel:
            listPhr.remove(listPhr[i])
        print(listPhr)
        '''
        #print(  [ n for n in self.okt.nouns(content) if len(n) > 1 ])


        print('------------------------------------------------')




