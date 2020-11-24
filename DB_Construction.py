import csv
from bs4 import BeautifulSoup
import requests
from pymongo import MongoClient
from pymongo import UpdateOne
from pymongo import InsertOne
import time
from datetime import datetime
import json
import re
import pandas as pd
import numpy as np

# 문자숫자 -> 숫자
def str_to_num(strnum):
    if strnum == '':
        return 0
    strnum = strnum.replace(',','')
    p = re.compile('[+-]?\d*(\.?\d*)$') # 양/음수를 포함 정수 실수 판별 정규식
    m = p.match(strnum)
    if m == None: return 'N/A'
    strnum = strnum.replace(',','').strip()
    if strnum.find('.') != -1:
        return float(strnum)
    return int(strnum)

# 다트 회사코드 정보 읽기
def dart_corp_code_data(filepath):
    fp = open(filepath, "r", encoding="UTF-8")
    soup = BeautifulSoup(fp, "lxml")
    dic_corp_code = dict()  # 종목코드 정보 dict 자료 선언
    for rslt in soup.find_all('list'):
        if rslt.find('stock_code').text.strip():   #종목코드가 있는 회사만 =>상장된 회사만 저장
            stock_code = rslt.find('stock_code').text
            dic_corp_code[stock_code] = rslt.find('corp_code').text #다트 종목코드
    return dic_corp_code

# KRX 일일 종목정보 파일 읽기
def read_KRX_stock_daily_info_file(filepath, market, date):
    f = open(filepath, 'r', encoding='utf-8')
    rows = csv.reader(f)
    listInfo=[]
    for i, row in enumerate(rows):
        if i == 0: continue
        stock_code = row[0]  # 종목코드
        stock_name = row[1]  # 종목명
        cur_price = int(row[2].replace(',', ''))  # 현재가
        pre_day_contrast = int(row[3].replace(',', ''))  # 전일대비
        fluctuation_rate = float(row[4])  # 등락률
        trading_vol = int(row[7].replace(',', ''))  # 거래량
        trading_val = int(row[8].replace(',', ''))  # 거래대금
        starting_price = int(row[9].replace(',', ''))  # 시가
        high_price = int(row[10].replace(',', ''))  # 고가
        low_price = int(row[11].replace(',', ''))  # 저가
        par_val = row[12].replace(',', '')  # 액면가
        if par_val.find('.') != -1:
            par_val = float(par_val)
        else:
            par_val = int(par_val)
        currency_div = row[13]  # 통화구분
        issued_shares_num = int(row[14].replace(',', ''))  # 발행주식수
        dic_data={'market': market,
                  'stock_code':stock_code,
                  'stock_name':stock_name,
                   'cur_price': cur_price,
                   #'pre_day_contrast': pre_day_contrast,
                   #'fluctuation_rate': fluctuation_rate,
                   #'trading_vol': trading_vol,
                   #'trading_val': trading_val,
                   #'starting_price': starting_price,
                   #'high_price': high_price,
                   #'low_price': low_price,
                   'par_val': par_val,
                   'currency_div': currency_div,
                   'issued_shares_num': issued_shares_num,
                   'price_date': date}
        listInfo.append(dic_data)
    return listInfo

# 종목 정보 구축
def constructDB_stock_corp_data(file_path, market, date):
    dic_dart_corp_code = dart_corp_code_data("CORPCODE.xml")
    list_stock_price = read_KRX_stock_daily_info_file(file_path, market, date)
    list_stock_crop = []
    for dic in list_stock_price:
        stock_code = dic['stock_code']
        if stock_code in dic_dart_corp_code:
            dic['dart_corp_code'] = dic_dart_corp_code[stock_code]
            dic['fs_data'] = False
            list_stock_crop.append(dic)

    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment
    STOCK_CROP_DATA_CLT = db["STOCK_CROP_DATA_CLT"]
    STOCK_CROP_DATA_CLT.insert_many(list_stock_crop)

# 종목정보 업데이트
def update_stock_corp_data(filepath, market, date):
    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment
    STOCK_CROP_DATA_CLT = db["STOCK_CROP_DATA_CLT"]
    YM_ASSET_STOCK_CTL = db["YM_ASSET_STOCK_CLT"]   #보유종목 컬렉션

    # 파일에서 오늘 종목 데이터(시세 등) 읽기
    list_today_corp_info = read_KRX_stock_daily_info_file(filepath, market, date)

    # 보유종목
    list_asset_stock = YM_ASSET_STOCK_CTL.find({}, {'_id':0, 'stock_code':1})
    set_asset_stock = set([x['stock_code'] for x in list_asset_stock])

    # DB에 저장된 종목 정보 얻기
    list_exist_corp_info = STOCK_CROP_DATA_CLT.find({'market':f'{market}'},{'_id':1, 'stock_code':1, 'stock_name':1})

    # 현재 종목과 오늘 종목을 비교해서 추가된 종목과 삭제된 종목을 얻는다.
    set_today_stock = set([x['stock_code'] for x in list_today_corp_info])
    set_exist_stock = set([x['stock_code'] for x in list_exist_corp_info])

    set_add_stock = set_today_stock - set_exist_stock
    set_del_stock = set_exist_stock - set_today_stock

    if len(set_del_stock):
        qRslt = STOCK_CROP_DATA_CLT.delete_many({'stock_code':{'$in':list(set_del_stock)}}) #$in 을 사용하기 위해서는 list로 형변환 해주어야함
        print(f'{len(qRslt.deleted_count)}개 종목 삭제: {set_del_stock}')

    if len(set_add_stock):
        #list_add_code = [x for x in set_add_stock]
        list_add_corp_info = [x for x in list_today_corp_info if x['stock_code'] in set_add_stock]
        for corp in list_add_corp_info:
            corp['fs_data'] = False
        qRslt = STOCK_CROP_DATA_CLT.insert_many(list_add_corp_info)
        print(f'{len(qRslt.inserted_ids)}개 종목 추가: {set_add_stock}')

    set_update_stock = set_today_stock - set_add_stock
    if len(set_update_stock):
        #list_update_code = [x['stock_code'] for x in set_update_stock]
        list_update_corp_info = [x for x in list_today_corp_info if x['stock_code'] in set_update_stock]

        '''
        bulk = STOCK_CROP_DATA_CLT.initialize_unordered_bulk_op()
        for doc in list_update_corp_info:
            bulk.find({'stock_code': doc['stock_code']}).update({'$set':{'cur_price': doc['cur_price'],
                                'issued_shares_num': doc['issued_shares_num'],
                                'price_date': date}})
        bulk.execute()
        '''
        # 주가정보 업데이트
        list_bulk_qry=[]
        list_asset_stock_qury=[] # 보유종목 주가 업데이트 쿼리
        for doc in list_update_corp_info:
            list_bulk_qry.append(
                UpdateOne({'stock_code': doc['stock_code']}, {'$set':{'cur_price': doc['cur_price'],
                                'issued_shares_num': doc['issued_shares_num'],
                                'price_date': date}})
            )

            # 해당 종목이 보유 종목이라면
            if doc['stock_code'] in set_asset_stock:
                list_asset_stock_qury.append(
                    UpdateOne({'stock_code':doc['stock_code']},
                              {
                                  '$set':{'cur_price':doc['cur_price'],'price_date': date}
                              })
                )

            if ( len(list_bulk_qry) == 1000 ):
                STOCK_CROP_DATA_CLT.bulk_write(list_bulk_qry,ordered=False)
                list_bulk_qry = []

        # 전체 종목 가격 업데이트
        if (len(list_bulk_qry) > 0):
            STOCK_CROP_DATA_CLT.bulk_write(list_bulk_qry, ordered=False)

        # 보유 종목 가격 업데이트
        if(len(list_asset_stock_qury) > 0):
            YM_ASSET_STOCK_CTL.bulk_write(list_asset_stock_qury, ordered=False)

    print(f'{len(set_update_stock)}개 종목 주가 업데이트')

    '''
    ========= tagId =========  
    divSonikY: 포괄손익계산서(년도)
    divSonikQ: 포괄손이계산서(분기)
    divDaechaY: 재무상태표(년도)
    divDaechaQ: 재무상태표(분기) 
    divCashY: 현금흐름표(년도)
    divCashQ: 현금흐름표(분기)
    =========================   
    '''
# 제무제표 각 파트별 데이터 얻기
def acquire_fs_part_data(soup, tagId, listPeriod, listFsPartData):
    # head tr 정보 얻기 => 기간 정보 획득
    fsPartTblHeadTr = soup.select(f'#{tagId} > table > thead > tr > th')
    colCnt = 0  # 획득 데이터 기간 갯수
    if tagId[-1] == 'Q':  # 분기일 경우
        colCnt = 4  # 컬럼 4개 (4개 분기 데이터) 데이터 조사
    else:
        colCnt = 3  # 컬럼 3개 (3년 데이터) 데이터 조사

    for i, th in enumerate(fsPartTblHeadTr):
        if i == 0: continue  # 첫번째 열 제외
        if i > colCnt: break  # 데이터 획득 컬럼을 넘어서면 LOOP 나오기

        if tagId[-1] == 'Q':   #분기일 경우
            listPeriod.append(th.text.strip()) # '년도/분기' 문자열 모두 저장
        else:
            listPeriod.append(int(th.text.strip().split('/')[0])) #'년도' 문자열만 저장

        # print(th.text.strip().split('/')[0])

    for i in listPeriod:
        listFsPartData.append({})

    # 계정항목 정보가 포함된 TR 태그 얻기
    accountTrs = soup.select(f'#{tagId} > table > tbody > tr')
    depStartAccntNm = ''  # 상세항목 포함 계정항목 이름
    #accntNo = 0
    #detailAccntNo = 0
    for i, tr in enumerate(accountTrs):
        # print(i+1,')항목:', tr.find('th').text.strip())
        td = tr.select('td')
        listVals = [x.text.strip() for x in td][0:colCnt]  # 기간별 계정항목 비용 값들

        # (상세항목이 없는)일반항목
        if 'rwf' in tr['class'] and \
                'acd_dep_start_close' not in tr['class'] and \
                'acd_dep2_sub' not in tr['class']:
            # print(tr.select('th>div')[0].text.strip(), listVals)
            accntNm = tr.select('th>div')[0].text.strip()
            #accntNo += 1
            #accntNm = accntNmPfx+f'_{accntNo}'
            for j, dic in enumerate(listFsPartData):
                dic[accntNm] = str_to_num(listVals[j])
        # (상세항목을 포함하는)항목
        elif 'acd_dep_start_close' in tr['class']:
            # print("+",tr.select('th>div>span')[0].text.strip(), listVals)
            accntNm = tr.select('th>div>span')[0].text.strip()
            #accntNo += 1
            #accntNm = accntNmPfx+f'_{accntNo}'
            #detailAccntNo = 0
            depStartAccntNm = accntNm + "_상세"
            for j, dic in enumerate(listFsPartData):
                dic[accntNm] = str_to_num(listVals[j])
                dic[depStartAccntNm] = {}
        # 상세항목
        elif 'acd_dep2_sub' in tr['class']:
            # print("-", tr.select('th')[0].text.strip(), listVals)
            accntNm = tr.select('th')[0].text.strip()
            #detailAccntNo += 1
            #accntNm = depStartAccntNm+f'_{detailAccntNo}'
            for j, dic in enumerate(listFsPartData):
                dic[depStartAccntNm][accntNm] = str_to_num(listVals[j])

# 추정 ROE 구하기
def estimateROE(val1, val2, val3):
    if val1 < val2 and val2 < val3:
        return val3
    if val1 > val2 and val2 > val3:
        return val3
    return (val1+val2*2+val3*3)/6

# 컨센서스 내용 파싱
def parse_consensus_contents(divId, soup):
    # 컨센서스 계정항목: 값
    dic_year = {}
    dic_quarter = {}
    # 컨센서스 헤더 => 년도 및 분기 컨센서스
    tagTrcnsHeader = soup.select(f'#{divId} > table > tbody > tr')
    for tr in tagTrcnsHeader:
        th = tr.select('th')
        accntNm = th[0].text.replace('(원)', '').strip()
        th_span = th[0].select('a > span')
        if th_span:
            accntNm = th_span[0].text.strip()
        tds = tr.select('td')

        if len(tds) < 8: break  # 컨센서스 데이터에서 최근3년 및 최근 3분기 전 데이터가 없다면 자료를 구축하지 않는다.

        yearVal = str_to_num(tds[3].text)
        quarterVal = str_to_num(tds[7].text)

        if yearVal and yearVal != 'N/A':
            dic_year[accntNm] = yearVal
        else:  # 컨센서스 데이터가 없다면 최근 년도 ROE 데이터만 활용
            if accntNm == 'ROE':  # 항목이 ROE라면 추정 ROE를 구한다.
                val1 = str_to_num(tds[0].text)
                val2 = str_to_num(tds[1].text)
                val3 = str_to_num(tds[2].text)
                if (val1 != 'N/A' and val2 != 'N/A' and val3 != 'N/A'):
                    yearVal = estimateROE(val1, val2, val3)
                    dic_year[accntNm] = yearVal
            elif accntNm == '자본총계' or accntNm == '지배주주지분':
                yearVal = str_to_num(tds[2].text)
                if yearVal != 'STRING':
                    dic_year[accntNm] = yearVal

        if quarterVal and quarterVal != 'N/A':
            dic_quarter[accntNm] = quarterVal
        else:  # 컨센서스 데이터가 없다면 최근 분기 ROE 데이터만 활용
            if accntNm == 'ROE':  # 항목이 ROE라면 추정 ROE를 구한다.
                val1 = str_to_num(tds[4].text)
                val2 = str_to_num(tds[5].text)
                val3 = str_to_num(tds[6].text)
                if (val1 != 'N/A' and val2 != 'N/A' and val3 != 'N/A'):
                    quarterVal = (val1+val2*2+val3*3)/6 # 분기 데이터 추정의 경우 평균 값 사용
                    dic_quarter[accntNm] = quarterVal
            elif accntNm == '자본총계' or accntNm == '지배주주지분':
                quarterVal = str_to_num(tds[6].text)
                if quarterVal != 'STRING':
                    dic_quarter[accntNm] = quarterVal
    print(json.dumps(dic_year, indent=4, sort_keys=False, ensure_ascii=False))
    #print(json.dumps(dic_quarter, indent=4, sort_keys=False, ensure_ascii=False))
    return dic_year, dic_quarter



# 컨센서스 데이터 크롤링
def crawling_fnGuide_consensus_basic_data(stock_code):
    URL = f"http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{stock_code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701"
    response = requests.get(URL)
    #with open(f'fnGuid_snapshot({stock_name}).html', 'r', encoding='utf-8') as file:
    #    htmlText = file.read()
    #soup = BeautifulSoup(htmlText, 'lxml')
    soup = BeautifulSoup(response.text, 'lxml')
    #with open(f'fnGuid_snapshot({stock_code}).html', 'w', encoding="utf-8") as outfile:
    #    outfile.write(soup.prettify())
    #return
    # 컨센서스 계정항목: 값

    #연결 재무정보
    dic_year, dic_quarter = parse_consensus_contents('highlight_D_A', soup)

    # 해당 종목이 별도 재무정보가

    if dic_year and dic_quarter:
        if dic_year['지배주주지분'] != 0 and dic_quarter['지배주주지분'] == 0:
            #별도 재무정보
            dic_year, dic_quarter = parse_consensus_contents('highlight_B_A', soup)

    # 자사주
    treasury_stock = soup.select('#svdMainGrid5 > table > tbody > tr:nth-child(5) > td:nth-child(3)')
    if treasury_stock:
        treasury_stock = treasury_stock[0].text
        treasury_stock = str_to_num(treasury_stock)
    else:
        treasury_stock = 0

    return dic_year, dic_quarter, treasury_stock

# 컨센서스 데이터 DB구축 보완
def supplement_constructDB_consensus_data():
    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment
    STOCK_CROP_DATA_CLT = db["STOCK_CROP_DATA_CLT"]
    stock_info = STOCK_CROP_DATA_CLT.find({'cns_year.매출액':{'$exists':False},'cns_year.영업이익':{'$exists':False}}) # 종목

    list_bulk=[]

    for i, stckInf in enumerate(stock_info):
        dic_year, dic_quarter, treasury_stock = crawling_fnGuide_consensus_basic_data(stckInf['stock_code'])
        # 분기 컨센서스 정보가 없다면 종목정보가 없음을 의미
        if dic_quarter:
            list_bulk.append([stckInf['stock_code'], dic_year, dic_quarter, treasury_stock])
            now = datetime.now()
            stock_nm = stckInf['stock_name']
            print(f'{i + 1}){stock_nm} crawling({now.year}-{now.month}-{now.day} {now.hour}:{now.minute}:{now.second})')
            time.sleep(0.5)

    list_bulk_qry=[]
    for item in list_bulk:
        list_bulk_qry.append(
            UpdateOne({'stock_code': item[0]}, {'$set':{'cns_year': item[1],
                            'cns_quarter': item[2], 'treasury_stock': item[3]}})
        )
        if ( len(list_bulk_qry) == 1000 ):
            STOCK_CROP_DATA_CLT.bulk_write(list_bulk_qry,ordered=False)
            print(f'{len(list_bulk_qry)})개 컨센서스 데이터 구축 ({now.year}-{now.month}-{now.day} {now.hour}:{now.minute}:{now.second})')
            list_bulk_qry = []

    if (len(list_bulk_qry) > 0):
        STOCK_CROP_DATA_CLT.bulk_write(list_bulk_qry, ordered=False)
        print(f'{len(list_bulk_qry)})개 컨센서스 데이터 구축 ({now.year}-{now.month}-{now.day} {now.hour}:{now.minute}:{now.second})')

# 컨센서스 데이터 DB구축
def constructDB_consensus_data():
    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment
    STOCK_CROP_DATA_CLT = db["STOCK_CROP_DATA_CLT"]
    stock_info = STOCK_CROP_DATA_CLT.find({}) # 종목

    list_bulk=[]

    for i, stckInf in enumerate(stock_info):        
        dic_year, dic_quarter, treasury_stock = crawling_fnGuide_consensus_basic_data(stckInf['stock_code'])
        # 분기 컨센서스 정보가 없다면 종목정보가 없음을 의미
        if dic_quarter:
            list_bulk.append([stckInf['stock_code'], dic_year, dic_quarter, treasury_stock])
            now = datetime.now()
            stock_nm = stckInf['stock_name']
            print(f'{i + 1}){stock_nm} crawling({now.year}-{now.month}-{now.day} {now.hour}:{now.minute}:{now.second})')
            time.sleep(0.5)

    list_bulk_qry=[]
    for item in list_bulk:
        list_bulk_qry.append(
            UpdateOne({'stock_code': item[0]}, {'$set':{'cns_year': item[1],
                            'cns_quarter': item[2], 'treasury_stock': item[3]}})
        )
        if ( len(list_bulk_qry) == 1000 ):
            STOCK_CROP_DATA_CLT.bulk_write(list_bulk_qry,ordered=False)
            print(f'{len(list_bulk_qry)})개 컨센서스 데이터 구축 ({now.year}-{now.month}-{now.day} {now.hour}:{now.minute}:{now.second})')
            list_bulk_qry = []

    if (len(list_bulk_qry) > 0):
        STOCK_CROP_DATA_CLT.bulk_write(list_bulk_qry, ordered=False)
        print(f'{len(list_bulk_qry)})개 컨센서스 데이터 구축 ({now.year}-{now.month}-{now.day} {now.hour}:{now.minute}:{now.second})')

# 재무제표 데이터 크롤링
def crawling_fnGuide_fs_data(stock_code, c_year = 0, c_quarter = 0, mode = 'a'):
    URL = "http://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A%s&cID=&MenuYn=Y&ReportGB=&NewMenuID=103&stkGb=701"%stock_code
    response = requests.get(URL)
    #with open('fnGuid_sample.html', 'r', encoding='utf-8') as file:
    #    htmlText = file.read()
    #soup = BeautifulSoup(htmlText, 'html.parser')
    soup = BeautifulSoup(response.text, 'html.parser')

    '''
    ========= tagId =========  
    divSonikY: 포괄손익계산서(년도)
    divSonikQ: 포괄손이계산서(분기)
    divDaechaY: 재무상태표(년도)
    divDaechaQ: 재무상태표(분기) 
    divCashY: 현금흐름표(년도)
    divCashQ: 현금흐름표(분기)
    =========================   
    '''
    listYearTagId=['divSonikY', 'divDaechaY', 'divCashY']
    listQuarterTagId = ['divSonikQ', 'divDaechaQ', 'divCashQ']
    listFsPartId=['포괄손익계산서','재무상태표','현금흐름표']

    listYearFsData=[]   #년도별 재무제표 데이터
    listQuarterFsData=[]    #분기별 재무제표 데이터

    #년도별 재무제표 데이터
    if mode == 'a' or mode == 'y': # 년도 재무제표 데이터 획득 모드일경우
        for i, tagId in enumerate(listYearTagId):
            #재무제표(년도)
            listYear=[] # 년도정보
            listYearData = []  # 년도별  재무제표 파트 데이터 ex)[2017년 손익계산서, 2018년 손익계산서, ...]
            acquire_fs_part_data(soup, tagId, listYear, listYearData) # 제무제표 각 파트 별 데이터 획득

            if i == 0:
                for year in listYear:
                    if c_year != 0 and year != c_year: continue # 특정 년도 제무재표 데이터만 획득일 경우 해당없는 년도 데이터는 스킵
                    dic_year={}
                    dic_year['year'] = year
                    listYearFsData.append(dic_year)

            for j, year in enumerate(listYear):
                if c_year == 0: # 모든 재무제표 데이터 획득 일경우
                    listYearFsData[j][listFsPartId[i]] = listYearData[j]
                else:   #특정년도 재무제표 데이터만 획득일 경우
                    if year == c_year: # 해당 재무제표 데이터 만 저장
                        listYearFsData[0][listFsPartId[i]] = listYearData[j]

    #print(json.dumps(listYearFsData[0], indent=4, sort_keys=False, ensure_ascii=False))

    #분기별 재무제표 데이터
    if mode == 'a' or mode == 'q':  # 분기 재무제표 데이터 획득 모드일경우
        for i, tagId in enumerate(listQuarterTagId):
            #재무제표(분기)
            listYearQuarter=[] # 년도/분기정보
            listQuarterData = []  # 분기별 재무제표 파트 데이터 ex)[2017년 손익계산서, 2018년 손익계산서, ...]
            acquire_fs_part_data(soup, tagId, listYearQuarter, listQuarterData)

            if i == 0:
                for yearQuarter in listYearQuarter:
                    dic_year={}
                    [year, quarter] = yearQuarter.split('/')
                    year = str_to_num(year)
                    quarter = int(str_to_num(quarter)/3)
                    if c_quarter != 0 and \
                            (year != c_year or quarter != c_quarter): continue # 특정 년도/분기 제무재표 데이터만 획득일 경우 해당없는 년도 데이터는 스킵
                    dic_year['year'] = year
                    dic_year['quarter'] = quarter
                    listQuarterFsData.append(dic_year)

            for j, yearQuarter in enumerate(listYearQuarter):
                [year, quarter] = yearQuarter.split('/')
                year = str_to_num(year)
                quarter = int(str_to_num(quarter) / 3)
                if c_quarter == 0: # 모든 재무제표 데이터 획득 일경우
                    listQuarterFsData[j][listFsPartId[i]] = listQuarterData[j]
                else: #특정년도 재무제표 데이터만 획득일 경우
                    if year == c_year and quarter == c_quarter: # 해당 재무제표 데이터 만 저장
                        listQuarterFsData[0][listFsPartId[i]] = listQuarterData[j]

    return listYearFsData, listQuarterFsData
    #print(json.dumps(listQuarterFsData[0], indent=4, sort_keys=False, ensure_ascii=False))
    
    #with open('fnGuid_sample.html', 'w', encoding="utf-8") as outfile:
    #    outfile.write(soup.prettify())

# 재무제표 데이터 DB구축
def constructDB_financialStatement_data( year = 0, quarter = 0, mode = 'a', target_stock_code='all'):
    '''
    year: 0 fn가이드에서 제공하는 모든 년도 재무제표 데이터 구축
         ex)2020: 2020년도 재무제표 데이터 구축
    quarter: 0 fn가이드에서 제공하는 모든 분기 재무제표 데이터 구축
            ex)3: 3분기 재무제표 데이터 구축
    mode: 'y': 년도 재무제표 데이터만 구축
          'q': 분기 재무제표 데이터만 구축
          'a': 년도/분기 재무제표 데이터 모두 구축
    '''

    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment
    STOCK_CROP_DATA_CLT = db["STOCK_CROP_DATA_CLT"]
    YEAR_FS_DATA_CLT = db["YEAR_FS_DATA_CLT"]
    QUARTER_FS_DATA_CLT = db["QUARTER_FS_DATA_CLT"]
    stock_info = STOCK_CROP_DATA_CLT.find({}) # 종목

    list_bulk_year=[];list_bulk_quarter=[]

    for i, stckInf in enumerate(stock_info):
        stock_code = stckInf['stock_code']

        # 특정 종목 획득일 경우 해당 종목이 아닌 데이터는 스킵한다.
        if target_stock_code != 'all' and \
                target_stock_code != stock_code: continue

        stock_name = stckInf['stock_name']
        listYearFsData, listQuarterFsData = crawling_fnGuide_fs_data(stckInf['stock_code'], year, quarter, mode)

        if len(listYearFsData):
            for x in listYearFsData:
                x['stock_code'] = stock_code
                x['stock_name'] = stock_name
                list_bulk_year.append(x)

        if len(listQuarterFsData):
            for x in listQuarterFsData:
                x['stock_code'] = stock_code
                x['stock_name'] = stock_name
                list_bulk_quarter.append(x)

        now = datetime.now()
        stock_nm = stckInf['stock_name']
        print(f'{i + 1}){stock_nm} 년도:{len(listYearFsData)}개,분기:{len(listQuarterFsData)}개 crawling({now.year}-{now.month}-{now.day} {now.hour}:{now.minute}:{now.second})')
        time.sleep(0.5)

    list_bulk_pair=[list_bulk_year, list_bulk_quarter]
    list_clt = [YEAR_FS_DATA_CLT, QUARTER_FS_DATA_CLT]
    list_data_name = ['년도 재무제표', '분기 재무제표']

    for i, list_bulk in enumerate(list_bulk_pair):
        list_bulk_qry=[]

        for item in list_bulk:
            list_bulk_qry.append(InsertOne(item))
            if ( len(list_bulk_qry) == 1000 ):
                list_clt[i].bulk_write(list_bulk_qry,ordered=False)
                print(f'{len(list_bulk_qry)})개 {list_data_name[i]} 데이터 구축 ({now.year}-{now.month}-{now.day} {now.hour}:{now.minute}:{now.second})')
                list_bulk_qry = []

        if (len(list_bulk_qry) > 0):
            list_clt[i].bulk_write(list_bulk_qry, ordered=False)
            print(f'{len(list_bulk_qry)})개 {list_data_name[i]} 데이터 구축 ({now.year}-{now.month}-{now.day} {now.hour}:{now.minute}:{now.second})')


# 재무제표 데이터 정제
def refining_financeStatement_data():
    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment
    STOCK_CROP_DATA_CLT = db["STOCK_CROP_DATA_CLT"]
    YEAR_FS_DATA_CLT = db["YEAR_FS_DATA_CLT"]
    QUARTER_FS_DATA_CLT = db["QUARTER_FS_DATA_CLT"]

    list_result = YEAR_FS_DATA_CLT.aggregate([
        {'$match': {'재무상태표.자산': 0}},
        {'$group': {'_id': '$stock_code', 'count': {'$sum': 1}}}
    ])

    for rslt in list_result:
        if int(rslt['count']) == 3:
            STOCK_CROP_DATA_CLT.update_one({'stock_code':rslt['_id']},{'$set':{'fs_data':False}})

    YEAR_FS_DATA_CLT.delete_many({'재무상태표.자산':0})
    QUARTER_FS_DATA_CLT.delete_many({'재무상태표.자산':0})

def insertOne_consensus_data(stock_code):
    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment
    STOCK_CROP_DATA_CLT = db["STOCK_CROP_DATA_CLT"]
    dic_year, dic_quarter, treasury_stock = crawling_fnGuide_consensus_basic_data(stock_code)
    # 분기 컨센서스 정보가 없다면 종목정보가 없음을 의미
    if dic_quarter:
        result = STOCK_CROP_DATA_CLT.update_one({'stock_code': stock_code}, {'$set':{'cns_year': dic_year,
                            'cns_quarter': dic_quarter, 'treasury_stock': treasury_stock}})
        print(result)

#update_stock_corp_data('D:\\강재규 업무\\증권\\종목\\전체종목정보\\KOSDAQ\\KOSDAQ_20200911.csv', 'KOSDAQ', '2020-09-11')

# 컬렉션 필드 삭제 쿼리
'''db.STOCK_CROP_DATA_CLT.update({}, 
{$unset:{pre_day_contrast:1,
    fluctuation_rate:1,
    trading_vol:1,
    trading_val:1,
    starting_price:1,
    high_price:1,
    low_price:1}},
    { multi: true })'''

# 제무비율 데이터 크롤링
def crawling_fnGuide_FR_data(stock_code, stock_name, c_year = 0, c_quarter = 0):
    #URL = f'http://comp.fnguide.com/SVO2/ASP/SVD_FinanceRatio.asp?pGB=1&gicode=A{stock_code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=104&stkGb=701'
    #response = requests.get(URL)
    #soup = BeautifulSoup(response.text, 'html.parser')
    #with open(f'./download/FR/fnGuid_{stock_name}_{stock_code}.html', 'w', encoding="utf-8") as outfile:
    #    outfile.write(soup.prettify())
    #return pd.DataFrame(), pd.DataFrame()

    with open(f'./download/FR/fnGuid_{stock_name}_{stock_code}.html', 'r', encoding='utf-8') as file:
        htmlText = file.read()
    soup = BeautifulSoup(htmlText, 'html.parser')

    # 제무비율 기간 컬럼헤드 ex) 2017/12, 2018/12, ...

    trs = soup.select('div.um_table > table > thead > tr')
    if len(trs) == 0:
        return pd.DataFrame(), pd.DataFrame()

    colHeadYear = soup.select('div.um_table > table > thead > tr')[0].select('th')
    colHeadQuat = soup.select('div.um_table > table > thead > tr')[1].select('th')
    # 제무비율 데이터 년도/분기 정보 => DataFrame 인덱스로 사용
    listYearIndex = [x.text.strip() for i, x in enumerate(colHeadYear) if i != 0]
    listQuatIndex = [x.text.strip() for i, x in enumerate(colHeadQuat) if i != 0]

    dicFR = {}  # 데이터 저장용 딕셔너리

    divs = soup.select('div.um_table')
    for i, div in enumerate(divs):
        if i == 1: # 년도 데이터 수집 완료, 분기데이터 수집 시작
            yKeys = dicFR.keys()
            listYearFrInfo = [pd.DataFrame(dicFR[x]) for x in yKeys]  # 제무비율 데이터 dictionary 형식 -> dataframe 변환
            for df in listYearFrInfo: df.index = listYearIndex  # 데이터 프레임에 index 값 설정, [기간정보, ex) 2016/12, 2017/12, ..]
            # 분기 제무비율 데이터 수집을 위해 데이터 저장용 딕셔너리 초기화
            dicFR = {}
        trs = div.select('table > tbody > tr')
        for tr in trs:
            if tr['class'][0] == 'tbody_tit':  # 제무비율 구분 타이틀(안정성, 성장성, 수익성, 활동성)
                ratioTitle = tr.select('th')[0].text.strip()
                #print(ratioTitle)

                # 제무비율 구분 타이틀로 키값 추가
                if ratioTitle not in dicFR:
                    dicFR[ratioTitle] = {}

            else:
                sub_rAccntNm = ''
                if len(tr['class']) > 1:
                    if tr['class'][1] == 'acd_dep_start_close':  # 항목명 / 항목 구성 요소 명
                        span = tr.select('th > div > div > a > span')
                        if len(span) > 0:
                            rAccntNm = tr.select('th > div > div > a > span')[0].text.strip()
                        else:
                            rAccntNm = tr.select('th > div > div')[0].text.strip()
                        rAccntNm = rAccntNm.replace('&nbsp;', '')
                        #print('  ' + rAccntNm)
                    elif tr['class'][2] == 'acd_dep2_sub':  # 서브 항목 명명
                        t1 = tr.select('th > div > dl > dt')
                        if len(t1) == 0:
                            t1 = tr.select('th > div')
                        sub_rAccntNm = t1[0].text.strip()
                        sub_rAccntNm = sub_rAccntNm.replace('\n', '')
                        sub_rAccntNm = sub_rAccntNm.replace(' ', '')
                        #print('    ' + sub_rAccntNm)
                else:
                    rAccntNm = tr.select('th > div')[0].text.strip()
                    #print('  ' + rAccntNm)

                # 항목의 년도 별 데이터 dictionary
                dicAccnt = dicFR[ratioTitle]
                # 항목 데이터 태그
                tds = tr.select('td')
                # 태그에서 텍스트 데이터 추출
                vals = [x.text.strip() for x in tds]

                # 값을 숫자형으로 변환('N/A', '흑전', '적지' 는 제외)
                for j, val in enumerate(vals):
                    if val != 'N/A' and val != '흑전' and val != '적지':
                        vals[j] = str_to_num(val)

                # 텍스트 데이터 년도별 dic 에 저장
                key = rAccntNm
                if sub_rAccntNm != '':  # 서브 타이틀이 존재한다면
                    key = rAccntNm + '_' + sub_rAccntNm  # 타이틀_서브타이틀 형식

                dicAccnt[key] = vals

    '''
    trs = soup.select('div.um_table > table > tbody > tr')
    ratioTitle = ''
    for i, tr in enumerate(trs):
        if tr['class'][0] == 'tbody_tit':  # 제무비율 구분 타이틀(안정성, 성장성, 수익성, 활동성)
            ratioTitle = tr.select('th')[0].text.strip()
            print(ratioTitle)

            # 제무비율 구분 타이틀로 키값 추가
            if ratioTitle not in dicFR:
                dicFR[ratioTitle] = {}
            else:  # 타이틀이 이미 존재한다면 => 년도 데이터 수집 완료, 분기데이터 수집 시작
                yKeys = dicFR.keys()
                listYearFrInfo = [pd.DataFrame(dicFR[x]) for x in yKeys]  # 제무비율 데이터 dictionary 형식 -> dataframe 변환
                for df in listYearFrInfo: df.index = listYearIndex  # 데이터 프레임에 index 값 설정, [기간정보, ex) 2016/12, 2017/12, ..]
                # 분기 제무비율 데이터 수집을 위해 데이터 저장용 딕셔너리 초기화
                dicFR = {}
                dicFR[ratioTitle] = {}
        else:
            sub_rAccntNm = ''
            if tr['class'][1] == 'acd_dep_start_close':  # 항목명 / 항목 구성 요소 명
                rAccntNm = tr.select('th > div > div > a > span')[0].text.strip()
                rAccntNm = rAccntNm.replace('&nbsp;', '')
                print('  ' + rAccntNm)
            elif tr['class'][2] == 'acd_dep2_sub':  # 서브 항목 명명
                t1 = tr.select('th > div > dl > dt')
                if len(t1) == 0:
                    t1 = tr.select('th > div')
                sub_rAccntNm = t1[0].text.strip()
                sub_rAccntNm = sub_rAccntNm.replace('\n', '')
                sub_rAccntNm = sub_rAccntNm.replace(' ', '')
                print('    ' + sub_rAccntNm)

            # 항목의 년도 별 데이터 dictionary
            dicAccnt = dicFR[ratioTitle]
            # 항목 데이터 태그
            tds = tr.select('td')
            # 태그에서 텍스트 데이터 추출
            vals = [x.text.strip() for x in tds]

            # 값을 숫자형으로 변환('N/A', '흑전', '적지' 는 제외)
            for i, val in enumerate(vals):
                if val != 'N/A' and val != '흑전' and val != '적지':
                    vals[i] = str_to_num(val)

            # 텍스트 데이터 년도별 dic 에 저장
            key = rAccntNm
            if sub_rAccntNm != '':  # 서브 타이틀이 존재한다면
                key = rAccntNm + '_' + sub_rAccntNm  # 타이틀_서브타이틀 형식

            dicAccnt[key] = vals
    '''

    #분기 제무비율 데이터
    yKeys = dicFR.keys()
    listQuatFrInfo = [pd.DataFrame(dicFR[x]) for x in yKeys]  # 제무비율 데이터 dictionary 형식 -> dataframe 변환
    for df in listQuatFrInfo: df.index = listQuatIndex  # 데이터 프레임에 index 값 설정, [기간정보, ex) 2016/3, 2016/6, ..]

    if c_year != 0: #데이터 모두 구축이 아니라면 => 특정 년도 데이터 만 구축
        # 지정 년도에 해당하는 재무비율 데이터 추출
        # 재무비율 데이터 프레임의 인덱스 값에서 지정 년도에 해당하는 인덱스(행)을 추출
        for y in listYearIndex:
            if y.find( str(c_year) ) == 0:
                listYearFrInfo = [x.loc[[y]] for x in listYearFrInfo]
                break

    if c_quarter != 0: #데이터 모두 구축이 아니라면 => 특정 분기 데이터 만 구축
        # 지정 분기에 해당하는 재무비율 데이터 추출
        # 재무비율 데이터 프레임의 인덱스 값에서 지정 년도에 해당하는 인덱스(행)을 추출
        for q in listQuatIndex:
            if q == (str(c_year)+'/%02d'%c_quarter):
                listQuatFrInfo = [x.loc[[y]] for x in listQuatFrInfo]
                break

    # 년도 제무비율 데이터 데이터 프레임의 열->인덱스, 인덱스->열 로 변경
    for i, df in enumerate(listYearFrInfo):
        df = df.stack() # 열 -> 인덱스
        listYearFrInfo[i] = df.unstack(0) #인덱스 ->열

    # 년도 제무비율 데이터(안정성비율, 성장성비율, 수익성비율, 활동성비율) 데이터를 하나의 데이터 프레임으로 합친다.(concat 함수 사용)
    dfYearFr = pd.DataFrame()
    if len(listYearFrInfo) > 0:
        dfYearFr = listYearFrInfo[0]
        for i, df in enumerate(listYearFrInfo):
            if i < len(listYearFrInfo)-1:
                dfYearFr = pd.concat([dfYearFr, listYearFrInfo[i+1]])

    # 분기도 제무비열 데이터 데이터 프레임의 열->인덱스, 인덱스->열 로 변경
    for i, df in enumerate(listQuatFrInfo):
        df = df.stack() # 열 -> 인덱스
        listQuatFrInfo[i] = df.unstack(0) #인덱스 ->열

    # 분기 제무비율 데이터(성장성비율, 수익성비율) 데이터를 하나의 데이터 프레임으로 합친다.(concat 함수 사용)
    dfQuatFr = pd.DataFrame()
    if len(listQuatFrInfo) > 0:
        dfQuatFr = listQuatFrInfo[0]
        for i, df in enumerate(listQuatFrInfo):
            if i < len(listQuatFrInfo)-1:
                dfQuatFr = pd.concat([dfQuatFr, listQuatFrInfo[i+1]])

    return dfYearFr, dfQuatFr

    # print(json.dumps(listQuarterFsData[0], indent=4, sort_keys=False, ensure_ascii=False))

    # with open('fnGuid_sample.html', 'w', encoding="utf-8") as outfile:
    #    outfile.write(soup.prettify())

# 재무비율 데이터 DB구축
def constructDB_financialRatio_data( year = 0, quarter=0, mode = 'a', target_stock_code='all', insert=True):
    '''
    year: 0 fn가이드에서 제공하는 모든 년도 재무비율 데이터 구축
         ex)2020: 2020년도 재무비율 데이터 구축
    quarter: 0 fn가이드에서 제공하는 모든 분기 재무비율 데이터 구축
            ex)3: 3분기 재무비율 데이터 구축
    mode: 'y': 년도 재무비율 데이터만 구축
          'q': 분기 재무비율 데이터만 구축
          'a': 년도/분기 재무비율 데이터 모두 구축
    '''

    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment
    STOCK_CROP_DATA_CLT = db["STOCK_CROP_DATA_CLT"]
    YEAR_FR_DATA_CLT = db["YEAR_FR_DATA_CLT"]   # 년도 제무비율 데이터
    QUARTER_FR_DATA_CLT = db["QUARTER_FR_DATA_CLT"] # 분기 간이 제무비율 데이터

    stock_info = STOCK_CROP_DATA_CLT.find({}) # 종목

    def bulk_query(listQuery, CLT, dfFr, mode, insert):
        if dfFr.empty == True: return

        listCols = dfFr.columns

        for col in listCols:
            df = dfFr.loc[:,[col]]
            dic = df.to_dict('dict')
            key = list(dic.keys())[0]
            dicFrVal = dic[key]
            dicFr = {}
            dicFr['stock_code'] = stock_code
            dicFr['stock_name'] = stock_name
            if key.find('/') != -1:
                year, mon = key.split('/')
            elif key.find('.') != -1:
                year, mon = key.split('.')
            dicFr['year'] = year
            dicFr['FR'] = dicFrVal
            if mode == 'q':
                dicFr['month'] = mon
            if insert == True:
                listQuery.append(InsertOne(dicFr))
            else:
                if mode == 'y':
                    listQuery.append(
                        UpdateOne({'stock_code': stock_code, 'year':year}, {'$set': {'FR': dicFrVal}})
                    )
                else:
                    listQuery.append(
                        UpdateOne({'stock_code': stock_code, 'year':year, 'month':mon}, {'$set': {'FR': dicFrVal}})
                    )

            if (len(listQuery) == 1000):
                CLT.bulk_write(listQuery, ordered=False)
                print(f'{len(listQuery)})개 데이터 구축')
                listQuery = []

        return listQuery

    listYearQuery = []
    listQuatQuery = []

    for i, stckInf in enumerate(stock_info):
        stock_code = stckInf['stock_code']
        stock_name = stckInf['stock_name']
        if stock_name.endswith('리츠') or stock_name.find('스팩') > 0:
            continue

        #if i < 1402: continue

        #if i == 1599: continue

        # 특정 종목 획득일 경우 해당 종목이 아닌 데이터는 스킵한다.
        if target_stock_code != 'all' and \
                target_stock_code != stock_code: continue

        print(f'{i+1}){stock_name}')
        # 년도, 분기 제무비율 정보 얻기
        dfYearFr, dfQuatFr = crawling_fnGuide_FR_data(stock_code, stock_name, year, quarter)

        if dfYearFr.empty == True and dfQuatFr.empty == True:
            continue

        if mode == 'y' or mode == 'a':
            listYearQuery = bulk_query(listYearQuery, YEAR_FR_DATA_CLT, dfYearFr, 'y', insert)

        if mode == 'q' or mode == 'a':
            listQuatQuery = bulk_query(listQuatQuery, QUARTER_FR_DATA_CLT, dfQuatFr, 'q', insert)

    if (len(listYearQuery) > 0):
        YEAR_FR_DATA_CLT.bulk_write(listYearQuery, ordered=False)
        print(f'{len(listYearQuery)})개 데이터 구축')

    if (len(listQuatQuery) > 0):
        QUARTER_FR_DATA_CLT.bulk_write(listQuatQuery, ordered=False)
        print(f'{len(listQuatQuery)})개 데이터 구축')

def downloadPage(filename, URL):
    #URL = f'http://comp.fnguide.com/SVO2/ASP/SVD_FinanceRatio.asp?pGB=1&gicode=A{stock_code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=104&stkGb=701'
    response = requests.get(URL)
    soup = BeautifulSoup(response.text, 'html.parser')
    with open(f'./download/FR/{filename}.html', 'w', encoding="utf-8") as outfile:
        outfile.write(soup.prettify())
    return

if __name__ == '__main__':
    constructDB_financialRatio_data(0, 0, 'a')
    #downloadPage('fnGuid_고바이오랩_348150', 'http://comp.fnguide.com/SVO2/ASP/SVD_FinanceRatio.asp?pGB=1&gicode=A290690&cID=&MenuYn=Y&ReportGB=&NewMenuID=104&stkGb=701')

