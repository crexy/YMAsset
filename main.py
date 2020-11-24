# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from DB_Construction import *
from Stock_value_Calc import *
from Stock_Info_Crawling import StockInfoCrawling

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # 종목 데이터 업데이트
    update_stock_corp_data('D:\\WORK\주식\\종목정보\\상장종목정보\\KOSPI\\KOSPI_20201123.csv', 'KOSPI', '2020-11-23')
    update_stock_corp_data('D:\\WORK\주식\\종목정보\\상장종목정보\\KOSDAQ\\KOSDAQ_20201123.csv', 'KOSDAQ', '2020-11-23')
    #updateAll_S_RIM()
    #S_RIM_low_valuation_stock_list('20201111')
    #S_RIM_low_valuation_stock_list_RndEx('20201119',8,2019,200,3)
    #RnD_Corp_Ranking(2019, 1000, 5)
    #AdvertisingCast_Corp_Ranking(2019, 1000, 4)
    #PersonnelExpense_Corp_Ranking(2019, 1000, 20) #인건비 비율 종목 킹

    # 한 종목 재무제표 데이터 구축
    #insertOne_financialStatement_data('126340')

    # 한 종목 컨센서스 데이터 구축
    #insertOne_consensus_data('002780')

    #constructDB_financialStatement_data()

    # 재무제표 데이터 정제
    #refining_financeStatement_data()

    #ROA_Rank100List(2020, 2, 200)

    #sic = StockInfoCrawling()
    #sic.crawlingData('게임빌', '063080')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
