from pymongo import MongoClient
from pymongo import UpdateOne
from operator import itemgetter
import csv
from scipy import stats

# 한글 문자열 변수 선언
CIS = "포괄손익계산서"
BS = "재무상태표"
CS = "현금흐름표"
SALES = "매출액"
CSH = "지배주주지분" #Controlling ShareHolder
PSHS = "지배기업주주지분" #Parent ShareHolder's Share
TOTCAP = "자본총계"
SnM_C_Dt = "판매비와관리비_상세"
RnD_C = "연구개발비"

# S-RIM 계산
def calc_S_RIM(bve, roe, k, w, cnt):
    '''
    :param bve: 지배주주지분 총자산
    :param roe: ROE
    :param k: 활인률
    :param w: 초과이익 지속계수
    :param cnt: 발행주식수
    :return: S-RIM 가격
    '''
    roe = roe/100
    k = k/100
    if roe-k >= 0: # 잉여이익이 양수인 경우
        s_rim = (bve + bve * (roe - k) * w / (1 + k - w)) / cnt
    else: # 잉여이익이 음수인 경우
        s_rim = ((bve + bve * (roe - k) / k) / cnt)*w
    return int(s_rim)

# S-RIM 업데이트
def updateOne_S_RIM(stock_code):
    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment
    STOCK_CROP_DATA_CLT = db["STOCK_CROP_DATA_CLT"]
    QUARTER_FS_DATA_CLT = db["QUARTER_FS_DATA_CLT"]

    #최근분기 지배주주지분 총자본 정보
    list_totalAsset = QUARTER_FS_DATA_CLT.find({'year':2020, 'quarter':2, 'stock_code':stock_code},{'_id':0,'stock_code':1, '재무상태표.지배기업주주지분':1})
    totalAsset = list_totalAsset[0][BS][PSHS]

    discntR = 8  # 할인율

    corp = STOCK_CROP_DATA_CLT.find({'stock_code':stock_code})[0]

    bve = 'N/A'; roe='N/A'

    issued_cnt = corp['issued_shares_num']
    treasury_cnt = 0
    if 'treasury_stock' in corp:
        treasury_cnt = corp['treasury_stock']

    if treasury_cnt == 'N/A': return
    if 'cns_year' not in corp: return

    dic_cns_year = corp['cns_year']
    if 'ROE' in dic_cns_year:
        if dic_cns_year['ROE'] != 'N/A':
            roe = dic_cns_year['ROE']

    shareCnt = issued_cnt - treasury_cnt

    if totalAsset:
        bve = totalAsset #최근분기 지배주주지분 총자본
    else:
        if CSH in dic_cns_year: # 지배주주지분
            bve = dic_cns_year[CSH]

    if(bve != 'N/A' and roe != 'N/A'):
        bve *= 100000000
        val100 = calc_S_RIM(bve, roe, discntR, 1, shareCnt)     # 적정가격
        val090 = calc_S_RIM(bve, roe, discntR, 0.9, shareCnt)   # 10% 할인가격
        val080 = calc_S_RIM(bve, roe, discntR, 0.8, shareCnt)   # 20% 할인가격

        result = STOCK_CROP_DATA_CLT.update_one({'stock_code': stock_code}, {'$set': {'S-RIM.100': val100, 'S-RIM.090': val090, 'S-RIM.080': val080}})
        print(result)



# S-RIM 업데이트
def updateAll_S_RIM():
    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment
    STOCK_CROP_DATA_CLT = db["STOCK_CROP_DATA_CLT"]
    QUARTER_FS_DATA_CLT = db["QUARTER_FS_DATA_CLT"]

    #최근분기 지배주주지분 총자본 정보
    list_totalAsset = QUARTER_FS_DATA_CLT.find({'year':2020, 'quarter':2},{'_id':0,'stock_code':1, '재무상태표.지배기업주주지분':1})

    for x in list_totalAsset:
        if PSHS not in x[BS]:
            print(x['stock_code'])

    #종목코드:지배주주지분 자산 map 생성
    dicTotalAsset = {x['stock_code']: x[BS][PSHS] for x in list_totalAsset}

    list_corp = STOCK_CROP_DATA_CLT.find({})

    discntR = 8 #할인율

    list_bulk = []
    rim_cnt = 1
    for doc in list_corp:
        bve = 'N/A'; roe='N/A'
        stock_code = doc['stock_code']
        if 'treasury_stock' not in doc:
            continue

        issued_cnt = doc['issued_shares_num']
        treasury_cnt = doc['treasury_stock']

        if treasury_cnt == 'N/A': continue
        if 'cns_year' not in doc: continue
        
        # 컨센서스 데이터가 존재하는 종목인 경우 년도 컨센서스의 ROE를 사용하고
        if SALES in doc['cns_year']:
            dic_cns_year = doc['cns_year']
            if 'ROE' in dic_cns_year:
                if dic_cns_year['ROE'] != 'N/A':
                    roe = dic_cns_year['ROE']
        else:   # 컨센서스 데이터가 존재하지 않는 종목은 분기 추정 ROE를 사용함
            dic_cns_quarter = doc['cns_quarter']
            if 'ROE' in dic_cns_quarter:
                if dic_cns_quarter['ROE'] != 'N/A':
                    roe = dic_cns_quarter['ROE']

        shareCnt = issued_cnt - treasury_cnt
        if stock_code in dicTotalAsset:
            bve = dicTotalAsset[stock_code] #최근분기 지배주주지분 총자본
        else:
            dic_cns_quarter = doc['cns_quarter']
            dic_cns_year = doc['cns_year']
            if CSH in dic_cns_quarter:
                bve = dic_cns_quarter[CSH]
            elif TOTCAP in dic_cns_quarter:
                bve = dic_cns_quarter[TOTCAP]
            elif CSH in dic_cns_year:
                bve = dic_cns_year[CSH]

        if(bve != 'N/A' and roe != 'N/A'):
            stock_name = doc['stock_name']
            #print(f'{rim_cnt}){stock_name}')
            bve *= 100000000
            val100 = calc_S_RIM(bve, roe, discntR, 1, shareCnt)     # 적정가격
            val090 = calc_S_RIM(bve, roe, discntR, 0.9, shareCnt)   # 10% 할인가격
            val080 = calc_S_RIM(bve, roe, discntR, 0.8, shareCnt)   # 20% 할인가격

            list_rim = [doc['stock_code'], val100, val090, val080]
            list_bulk.append(list_rim)

            #print(f'{rim_cnt}){stock_name}: {list_rim}')
            rim_cnt += 1

    list_bulk_qry=[]
    for item in list_bulk:
        list_bulk_qry.append(
            UpdateOne({'stock_code': item[0]}, {'$set':{'S-RIM.100': item[1],'S-RIM.090':item[2],'S-RIM.080':item[3]}})
        )
        if ( len(list_bulk_qry) == 1000 ):
            STOCK_CROP_DATA_CLT.bulk_write(list_bulk_qry,ordered=False)
            print(f'{len(list_bulk_qry)})개 S-RIM 데이터 업데이트')
            list_bulk_qry = []

    if (len(list_bulk_qry) > 0):
        STOCK_CROP_DATA_CLT.bulk_write(list_bulk_qry, ordered=False)
        print(f'{len(list_bulk_qry)})개 S-RIM 데이터 업데이트')

# S-RIM으로 저평가 종목 리스트 얻기
def S_RIM_low_valuation_stock_list(today):
    # rrr (required reate of return) 요구수익률
    savefilepath = f'D:\\WORK\\주식\\종목정보\\S-RIM종목\\SRIM({today}).csv'
    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment
    STOCK_CROP_DATA_CLT = db["STOCK_CROP_DATA_CLT"]

    listCorp = STOCK_CROP_DATA_CLT.find({})

    rows=[]

    for corp in listCorp:
        stock_name = corp['stock_name']
        if stock_name.find('홀딩스') != -1 or \
                stock_name.find('지주') != -1 or \
                stock_name.find('리츠') != -1 or \
                stock_name.find('스팩') != -1:
            continue    #홀딩스(지주회사) / 리츠 는 제외
        stock_code = corp['stock_code']
        price = corp['cur_price']
        if 'S-RIM' not in corp: continue
        srim080 = int(corp['S-RIM']['080'])
        srim090 = int(corp['S-RIM']['090'])
        srim100 = int(corp['S-RIM']['100'])

        if srim080 > price:

            separationRate = int(price / srim080 * 100)
            #if separationRate < 80: continue
            #print(f'{stock_name},{stock_code},{price},' + '%d,%d,%d' %(srim080,srim090,srim100))

            stock_code = "'%s" %corp['stock_code']
            rows.append([0, stock_name, stock_code, price, srim080, srim090, srim100, separationRate])

    rows.sort(key=lambda x:x[7], reverse=False)
    #rows.sort(key=itemgetter(7), reverse=True)

    no = 0
    for row in rows:
        no += 1;row[0] = no

    rows.insert(0, ['번호','종목명','종목코드','현재가','RIM80','RIM90','RIM100','RIM80 괴리율'])

    with open(savefilepath, 'wt', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)
        print(f'{no}개 종목 저장 {savefilepath}')

def S_RIM_low_valuation_stock_list_RndEx(today,rrr,year,salesLimit,gtePercent):
    # rrr (required reate of return) 요구수익률
    savefilepath = f'D:\\WORK\\주식\\종목정보\\S-RIM종목\\{today}_요구수익률({rrr})_RnD_{year}_{salesLimit}억_{gtePercent}P.csv'
    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment
    STOCK_CROP_DATA_CLT = db["STOCK_CROP_DATA_CLT"]

    listCorp = STOCK_CROP_DATA_CLT.find({'cns_year.ROE':{'$gt':rrr}})

    setRndCode = RnD_Corp_Ranking_codeSet(year, salesLimit, gtePercent)

    rows=[]

    for corp in listCorp:
        stock_name = corp['stock_name']
        stock_code = corp['stock_code']
        price = corp['cur_price']
        srim080 = int(corp['S-RIM']['080'])
        srim090 = int(corp['S-RIM']['090'])
        srim100 = int(corp['S-RIM']['100'])

        if srim080 > price:

            if stock_code not in setRndCode: continue

            separationRate = int(price / srim080 * 100)
            #if separationRate < 80: continue
            #print(f'{stock_name},{stock_code},{price},' + '%d,%d,%d' %(srim080,srim090,srim100))

            stock_code = "'%s" %corp['stock_code']
            rows.append([0, stock_name, stock_code, price, srim080, srim090, srim100, separationRate])

    rows.sort(key=lambda x:x[7], reverse=False)
    #rows.sort(key=itemgetter(7), reverse=True)

    no = 0
    for row in rows:
        no += 1;row[0] = no

    rows.insert(0, ['번호','종목명','종목코드','현재가','RIM80','RIM90','RIM100','RIM80 괴리율'])

    with open(savefilepath, 'wt', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)
        print(f'{no}개 종목 저장 {savefilepath}')

# 매출액 미만 종목 코드 셋 얻기
def sale_Less_CorpCodeSet(sales, year):
    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment
    YEAR_FS_DATA_CLT = db["YEAR_FS_DATA_CLT"]
    listCorp = YEAR_FS_DATA_CLT.find({'year':year,'포괄손익계산서.매출액':{'$lt':sales}})
    setCode = ([x['stock_code'] for x in listCorp])
    return setCode

# R&D 비율 종목 랭킹 종목코드 셋 얻기
def RnD_Corp_Ranking_codeSet(year, salesLimit, gtePercent):
    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment
    YEAR_FS_DATA_CLT = db["YEAR_FS_DATA_CLT"]

    # 종목코드, 종목명, 매출액, 연구개발비
    listCorp = YEAR_FS_DATA_CLT.find({'year': year},{'_id':0, 'stock_code':1, 'stock_name':1,f'{CIS}.{SALES}':1,f'{CIS}.{SnM_C_Dt}.{RnD_C}':1})

    setStockCode = ([])

    for corp in listCorp:
        #print(corp['stock_name'],corp['stock_code'])
        if not SnM_C_Dt in corp[CIS] : continue
        if corp[CIS][SALES] < salesLimit : continue
        rndP = int(corp[CIS][SnM_C_Dt][RnD_C] / corp[CIS][SALES] * 100)
        if rndP >= gtePercent:
            setStockCode.append(corp['stock_code'])
    return setStockCode            


# 판관비 하위항목 랭킹 종목 얻기
def SAE_subItem_Ranking_Stock(year, salesLimit, gtePercent, subGroupItem):
    group = CIS   #포괄손익계산서 그룹
    salesItem = SALES #매출액 항목
    subGroup = SnM_C_Dt   #판관비 그룹
    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment
    STOCK_CROP_DATA_CLT = db["STOCK_CROP_DATA_CLT"]
    YEAR_FS_DATA_CLT = db["YEAR_FS_DATA_CLT"]

    listCorp = STOCK_CROP_DATA_CLT.find({})
    dicSRIM={}
    for corp in listCorp:
        if 'S-RIM' in corp:
            price = corp['cur_price']
            S80 = corp['S-RIM']['080']
            S90 = corp['S-RIM']['090']
            S100 = corp['S-RIM']['100']
            dicSRIM[corp['stock_code']] = [price,S80,S90,S100]

    salesField = f'{group}.{salesItem}' # '포괄손익계산서.매출액' 매출액 필드
    subGroupItemField = f'{group}.{subGroup}.{subGroupItem}'    # 판관비 항목 필드
    # 종목코드, 종목명, 매출액, 판관비 서브항목
    listCorp = YEAR_FS_DATA_CLT.find({'year': year}, {'_id': 0, 'stock_code': 1, 'stock_name': 1, salesField: 1,
                                                      subGroupItemField: 1})

    rows = []

    for corp in listCorp:
        # print(corp['stock_name'],corp['stock_code'])
        if not subGroup in corp[group]: continue
        sales = corp[group][salesItem] # 매출액
        if sales < salesLimit: continue

        # 항목 값
        itemVal = corp[group][subGroup][subGroupItem]

        # 항목 매출액 비율
        subItemPercent = int(itemVal / sales * 100)
        if subItemPercent >= gtePercent:
            stock_code = corp['stock_code']
            price=0; S80=0; S90=0; S100=0; separationRate=0

            if stock_code in dicSRIM:
                listSRIM = dicSRIM[stock_code]
                price = listSRIM[0]
                S80 = int(listSRIM[1])
                S90 = int(listSRIM[2])
                S100 = int(listSRIM[3])

                if S100 != 0:
                    separationRate = int(price / S100 * 100)
                else:
                    separationRate = 'N/A'

            stock_code = "'%s" % corp['stock_code']
            rows.append(
                [corp['stock_name'], stock_code, sales, itemVal, subItemPercent, price, S80, S90, S100, separationRate])

    rows.sort(key=lambda x: x[4], reverse=True)

    for i, row in enumerate(rows):
        row.insert(0, i + 1)

    return rows

# R&D 비율 종목 랭킹 얻기
def RnD_Corp_Ranking(year, salesLimit, gtePercent):
    rows = SAE_subItem_Ranking_Stock(year, salesLimit, gtePercent, '연구개발비')
    savefilepath = f'D:\\WORK\\주식\\종목정보\\연구개발비_{year}_{salesLimit}억gt_{gtePercent}Pgt.csv'

    rows.insert(0, ['번호', '종목명','종목코드',SALES,'연구개발비','비율','현재가','RIM80','RIM90','RIM100','RIM100 괴리율'])
    cnt = len(rows)
    with open(savefilepath, 'wt', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)
        print(f'{cnt}개 종목 저장 {savefilepath}')

#광고선전비 비율 종목 랭킹
def AdvertisingCast_Corp_Ranking(year, salesLimit, gtePercent):
    rows = SAE_subItem_Ranking_Stock(year, salesLimit, gtePercent, '광고선전비')

    savefilepath = f'D:\\WORK\\주식\\종목정보\\광고선전비_{year}_{salesLimit}억gt_{gtePercent}Pgt.csv'

    rows.insert(0, ['번호', '종목명','종목코드',SALES,'광고선전비','비율', '현재가','RIM80','RIM90','RIM100','RIM100 괴리율'])
    cnt = len(rows)
    with open(savefilepath, 'wt', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)
        print(f'{cnt}개 종목 저장')

#인건비 비율 종목 킹
def PersonnelExpense_Corp_Ranking(year, salesLimit, gtePercent):
    rows = SAE_subItem_Ranking_Stock(year, salesLimit, gtePercent, '인건비')

    savefilepath = f'D:\\WORK\\주식\\종목정보\\인건비_{year}_{salesLimit}억gt_{gtePercent}Pgt.csv'

    rows.insert(0, ['번호', '종목명','종목코드',SALES,'인건비','비율','현재가','RIM80','RIM90','RIM100','RIM100 괴리율'])
    cnt = len(rows)
    with open(savefilepath, 'wt', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)
        print(f'{cnt}개 종목 저장')

#총자산 회전률 리스트 구하기
def turnOverRateOfTotalAssetList(year, quarter, salesLimit):
    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment
    QUARTER_FS_DATA_CLT = db["QUARTER_FS_DATA_CLT"]

#S-RIM 데이터 얻기
def S_RIM_Data(db):
    STOCK_CROP_DATA_CLT = db["STOCK_CROP_DATA_CLT"]
    listCorp = STOCK_CROP_DATA_CLT.find({'S-RIM':{'$exists':True}})
    dic={}
    for corp in listCorp:
        stock_code = corp['stock_code']
        price = corp['cur_price']
        srim080 = int(corp['S-RIM']['080'])
        srim090 = int(corp['S-RIM']['090'])
        srim100 = int(corp['S-RIM']['100'])
        if srim080 != 0:
            separationRate = int(price / srim080 * 100)
        else:
            separationRate = 'N/A'
        dic[stock_code]=[price, srim080, srim090, srim100, separationRate]
    return dic



#매출채권 회전율 Ranking 100 리스트 구하기
def accountReceivableTurnoverRank100List(year, quarter, salesLimit):
    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment

    QUARTER_FS_DATA_CLT = db["QUARTER_FS_DATA_CLT"]

    listQuarFsData = QUARTER_FS_DATA_CLT.find({'재무상태표.유동자산_상세.매출채권및기타유동채권':{'$exists':True}})

    dicSRIM = S_RIM_Data(db)

    dicAccntRcvbl= {}
    for qfs in listQuarFsData:
        stockName = qfs['stock_name']
        stockCode = qfs['stock_code']

        fsYear = qfs['year']
        fsQuarter = qfs['quarter']

        quarDif = quarter - 4

        bAppend = False
        # 지정 분기 기준 1년 범위에서 벗어난 데이터는 필터링
        if quarDif == 0 and fsYear == year: bAppend = True #4분기 경우
        elif quarDif < 0: # 1~3분기 경우
            if fsYear == year and fsQuarter <= quarter: bAppend = True
            elif fsYear == year -1 and fsQuarter >= 5+quarDif: bAppend = True

        if bAppend == True:
            if stockCode in dicAccntRcvbl:
                dicAccntRcvbl[stockCode].append(qfs)
                print(f'{stockName} {fsYear} {fsQuarter} 추가')
            else:
                dicAccntRcvbl[stockCode] = [qfs]
                print(f'{stockName} {fsYear} {fsQuarter} 입력')

    tupleStockCode = dicAccntRcvbl.keys()

    rows=[]

    for stockCode in tupleStockCode:
        listQFS = dicAccntRcvbl[stockCode]
        sales = 0;
        accntRcvbl = 0;
        for qfs in listQFS:
            #매출액
            sales += qfs[CIS][SALES]
            # 매출채권(account receivable)
            accntRcvbl += qfs[BS]['유동자산_상세']['매출채권및기타유동채권']
            # 매출채권 회전율(account receivable turnover)

        if sales < salesLimit: continue

        stockName = listQFS[0]['stock_name']
        accntRcvbl /= len(listQFS) # 매출채권 평균값
        accntRcvbl_turnover = (sales / accntRcvbl)
        list = [stockName, "'%s"%stockCode, sales, accntRcvbl, accntRcvbl_turnover]
        if stockCode in dicSRIM:
            listSRIM = dicSRIM[stockCode]
            for x in listSRIM: list.append(x)
        rows.append(list)

    rows.sort(key=lambda x: x[4], reverse=True)

    if len(rows) > 100:
        rows = rows[0:100]

    for i, row in enumerate(rows):
        row.insert(0, i + 1)

    savefilepath = f'D:\\WORK\\주식\\종목정보\\{year}_{quarter}_매출채권회전률_Ranking100.csv'
    rows.insert(0, ['번호', '종목명','종목코드',SALES,'매출채권','매출채권회전률','현재가','RIM80','RIM90','RIM100','RIM80 괴리율'])
    cnt = len(rows)-1
    with open(savefilepath, 'wt', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)
        print(f'{cnt}개 종목 저장')

#분기 재무제표 그룹 데이터: 동일 종목별 분기 재무제표 데이터를 종목코드를 키값으로 갖는 디셔너리 데이터로 반환
def quarterFsDataGroup(year, quarter, listQuarFsData):
    dicQuarterFsGroup = {}  # 분기 그룹 재무데이터

    for qfs in listQuarFsData:
        stockName = qfs['stock_name']
        stockCode = qfs['stock_code']

        fsYear = qfs['year']
        fsQuarter = qfs['quarter']

        quarDif = quarter - 4

        bAppend = False
        # 지정 분기 기준 1년 범위에서 벗어난 데이터는 필터링
        if quarDif == 0 and fsYear == year:
            bAppend = True  # 4분기 경우
        elif quarDif < 0:  # 1~3분기 경우
            if fsYear == year and fsQuarter <= quarter:
                bAppend = True
            elif fsYear == year - 1 and fsQuarter >= 5 + quarDif:
                bAppend = True

        if bAppend == True:
            if stockCode in dicQuarterFsGroup:
                dicQuarterFsGroup[stockCode].append(qfs)
                print(f'{stockName} {fsYear} {fsQuarter} 추가')
            else:
                dicQuarterFsGroup[stockCode] = [qfs]
                print(f'{stockName} {fsYear} {fsQuarter} 입력')
        
    return dicQuarterFsGroup

#ROA Ranking 100 리스트 구하기
def ROA_Rank100List(year, quarter, salesLimit):
    client = MongoClient("localhost", 27017)
    db = client.Stock_Investment
    QUARTER_FS_DATA_CLT = db["QUARTER_FS_DATA_CLT"]

    dicSRIM = S_RIM_Data(db)

    listQuarFsData = QUARTER_FS_DATA_CLT.find({'포괄손익계산서.당기순이익':{'$exists':True}}) #순이익이 있는 자료
    #listQuarFsData = QUARTER_FS_DATA_CLT.find({})  # 순이익이 있는 자료

    # 종목별 분기 재무제표 데이터
    dicQuarterFsGroup = quarterFsDataGroup(year, quarter, listQuarFsData)
    # 종목코드
    tupleStockCode = dicQuarterFsGroup.keys()

    rows=[]

    for stockCode in tupleStockCode:
        listQFS = dicQuarterFsGroup[stockCode]

        stockName = listQFS[0]['stock_name']
        if stockName.find('홀딩스') != -1 or \
                stockName.find('지주') != -1 or \
                stockName.find('리츠') != -1 or \
                stockName.find('스팩') != -1:
            continue    #홀딩스(지주회사) / 리츠 는 제외

        if SALES not in listQFS[0][CIS]: continue

        sales_sum = 0;  #매출액
        netProfit_sum = 0;  #순이익
        asset_sum = 0; #자산
        equity_sum = 0;  # 자기자본
        cash_asset_sum = 0; #현금성 자산
        current_liabilities_sum = 0 #유동부채
        tax_sum = 0 #법인세
        interest_cost_sum = 0#이지비용

        listROA = []   #ROA 리스트
        listROE = []   #ROE 리스트

        for qfs in listQFS:
            sales_sum += qfs[CIS][SALES] # 매출액
            
            if '지배주주순이익' in qfs[CIS]:
                netProfit = qfs[CIS]['지배주주순이익'] #지배주주순이익
            else:
                netProfit = qfs[CIS]['당기순이익']  # 당기순이익

            netProfit_sum += netProfit

            if '지배기업주주지분' in qfs[BS]:
                equity = qfs[BS]['지배기업주주지분']
            else:
                equity = qfs[BS]['자본']

            equity_sum += equity

            asset = qfs[BS]['자산'] # 자산
            asset_sum += asset
            
            #분기 ROA / ROE 구하기
            qROA = netProfit / asset
            qROE = netProfit / equity
            listROA.append(qROA)
            listROE.append(qROE)
            
            # 현금성자산
            cash_asset_sum += qfs[BS]['유동자산_상세']['유동금융자산']
            cash_asset_sum += qfs[BS]['유동자산_상세']['기타유동자산']
            cash_asset_sum += qfs[BS]['유동자산_상세']['현금및현금성자산']
            cash_asset_sum += qfs[BS]['유동자산_상세']['매각예정비유동자산및처분자산집단']
            #유동부채
            current_liabilities_sum += qfs[BS]['유동부채']
            #법인세비용
            tax_sum += qfs[CIS]['법인세비용']
            #이자비용
            interest_cost_sum += qfs[CIS]['금융원가_상세']['이자비용']

        if sales_sum < salesLimit: continue
        
        #자산평균
        asset_avg = asset_sum / len(listQFS)
        #자기자본 평균
        equity_avg = equity_sum / len(listQFS)

        # ROA
        ROA = (netProfit_sum / asset_avg)
        # ROE
        ROE = (netProfit_sum / equity_avg)

        x = range(0, len(listQFS))

        slope, intercept, r_value, p_value, stderr = stats.linregress(x, listROA)
        '''
        slope
        회귀선의 기울기입니다.
        intercept
        회귀선의 절편입니다.
        rvalue
        상관 계수.
        pvalue
        귀무 가설이 있는 가설 검정의 양측 p-값 기울기가 0인지 여부, t-분포와 함께 Wald Test를 사용합니다. 검정 통계량
        stderr
        추정된 그라데이션의 표준 오차입니다.        
        '''
        predicROA = slope*len(listQFS)+intercept;

        slope, intercept, r_value, p_value, stderr = stats.linregress(x, listROE)
        predicROE = slope * len(listQFS) + intercept;

        if ROA > predicROA: continue
        if ROE > predicROE: continue

        # 총자산회전율
        asset_turnover_ratio = sales_sum / asset_sum
        asset_turnover_ratio = format(asset_turnover_ratio, ".2f")
        # 현금비율
        cashRatio = current_liabilities_sum / cash_asset_sum
        cashRatio = format(cashRatio, ".2f")
        #이자보상비율
        if netProfit_sum+tax_sum+interest_cost_sum != 0:
            interest_coverage_ratio = interest_cost_sum / (netProfit_sum+tax_sum+interest_cost_sum)
            interest_coverage_ratio = format(interest_coverage_ratio, ".2f")
        else:
            interest_coverage_ratio = "N/A"

        list = [stockName, "'%s"%stockCode, sales_sum, netProfit_sum, asset_avg, ROA, ROE, predicROA, predicROE, asset_turnover_ratio, cashRatio, interest_coverage_ratio]
        if stockCode in dicSRIM:
            listSRIM = dicSRIM[stockCode]
            for x in listSRIM: list.append(x)
        rows.append(list)

    rows.sort(key=lambda x: x[5], reverse=True)

    if len(rows) > 100:
        rows = rows[0:100]

    for i, row in enumerate(rows):
        row.insert(0, i + 1)

    savefilepath = f'D:\\WORK\\주식\\종목정보\\{year}_{quarter}_ROA_Ranking100.csv'
    rows.insert(0, ['번호', '종목명','종목코드',SALES,'순이익','자산(평균)','ROA', 'ROE', '예측ROA', '예측ROE', '총자산회전율','현금비율','이자보상비율','현재가','RIM80','RIM90','RIM100','RIM80 괴리율'])
    cnt = len(rows)-1
    with open(savefilepath, 'wt', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)
        print(f'{cnt}개 종목 저장')



#accountReceivableTurnoverRank100List(2020, 2, 1000)


