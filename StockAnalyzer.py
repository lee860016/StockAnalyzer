import tushare as ts
import baostock as bs
import akshare as ak
import pandas as pd 
import mplfinance as mpf
import xlrd, time, sys, logging
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_analyzer.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 股票市场定义
markets = {
    '1': {
        'name': '上证主板', 
        'exchage': 'sh',
        'code_prefix': '60',
        'description': '上海证券交易所主板市场'
    },
    '2': {
        'name': '上证科创板',
        'exchage': 'sh',
        'code_prefix': '68',
        'description': '上海证券交易所科创板'
    },
    '3': {
        'name': '深证主板',
        'exchage': 'sz',
        'code_prefix': '00', 
        'description': '深圳证券交易所主板市场'
    },
    '4': {
        'name': '深证创业板',
        'exchage': 'sz',
        'code_prefix': '30',
        'description': '深圳证券交易所创业板'
    },
    '5': {
        'name': '北证',
        'exchage': 'bj',
        'code_prefix': '920',
        'description': '北京证券交易所'
    }
}

df_SSE_Main = pd.DataFrame()
df_SSE_STAR = pd.DataFrame()
df_SZSE_Main = pd.DataFrame()
df_SZSE_ChiNext = pd.DataFrame()
df_BSE = pd.DataFrame()

# 北交所股票列表
tsstocklist = list()
bse_recommend_stocks = dict()

# 上交所、深交所
bsstocklist = list()
recommend_stocks = dict()

# TUSHARE接口令牌设置
token = '8b001669116f59aed7f94ef845ec0a9be810ac310df5b7e2f4147b93'
ts.set_token(token)

# 从AKShare接口获取上交所、深交所和北交所股票列表
def GetStockListByAKShare():

    #
    global df_SSE_Main, df_SSE_STAR, df_SZSE_Main, df_SZSE_ChiNext, df_BSE

    # 获取上交所股票-主板
    df_SSE_Main = ak.stock_info_sh_name_code(symbol="主板A股")
    # 重命名列以便统一处理
    df_SSE_Main = df_SSE_Main.rename(columns={
        '证券代码': 'stock_code',
        '证券简称': 'stock_name',
        '上市日期': 'listing_date'
    })
    df_SSE_Main['market_type'] = '主板A股'
    df_SSE_Main['stock_code_full'] = df_SSE_Main['stock_code'] + '.SH'
    print(f"获取到 {len(df_SSE_Main)} 只上交所主板A股股票")
    #print(gem_stocks.head())
    #print(df_SSE_Main)

    # 获取上交所股票-科创板
    df_SSE_STAR = ak.stock_info_sh_name_code(symbol="科创板")
    # 重命名列以便统一处理
    df_SSE_STAR = df_SSE_STAR.rename(columns={
        '证券代码': 'stock_code',
        '证券简称': 'stock_name',
        '上市日期': 'listing_date'
    })
    df_SSE_STAR['market_type'] = '科创板'
    df_SSE_STAR['stock_code_full'] = df_SSE_STAR['stock_code'] + '.SH'
    print(f"获取到 {len(df_SSE_STAR)} 只科创板股票")
    #print(df_SSE_STAR)

    # 获取深交所股票
    stock_info_sz_df = ak.stock_info_sz_name_code(symbol="A股列表")
    # 重命名列以便统一处理
    stock_info_sz_df = stock_info_sz_df.rename(columns={
        'A股代码': 'stock_code',
        'A股简称': 'stock_name',
        'A股上市日期': 'listing_date',
        '板块': 'market_type'
    })
    stock_info_sz_df['stock_code_full'] = stock_info_sz_df['stock_code'] + '.SZ'
    # 分别提取主板和创业板
    condition = stock_info_sz_df['stock_code'].str.startswith('00')
    df_SZSE_Main = stock_info_sz_df[condition].copy()
    condition = stock_info_sz_df['stock_code'].str.startswith('30')
    df_SZSE_ChiNext = stock_info_sz_df[condition].copy()
    # 
    print(f"获取到 {len(stock_info_sz_df)} 只深交所A股股票，其中主板{len(df_SZSE_Main)}只，创业板{len(df_SZSE_ChiNext)}只")
    #print(df_SZSE_Main)
    #print(df_SZSE_ChiNext)

    # 获取北交所股票
    df_BSE = ak.stock_info_bj_name_code()
    # 重命名列以便统一处理
    df_BSE = df_BSE.rename(columns={
        '证券代码': 'stock_code',
        '证券简称': 'stock_name',
        '上市日期': 'listing_date'
    })
    df_BSE['market_type'] = ''
    df_BSE['stock_code_full'] = df_BSE['stock_code'] + '.BJ'
    print(f"获取到 {len(df_BSE)} 只北交所股票")
    #rint(df_BSE)

# 从AKShare接口获取上交所、深交所和北交所股票日交易数据
def GetStockDataByAKShare():

    #
    global df_SSE_Main, df_SSE_STAR, df_SZSE_Main, df_SZSE_ChiNext, df_BSE

    start_date = '20251117'
    end_date = '20251121'

    dfs = [df_SSE_Main, df_SSE_STAR, df_SZSE_Main, df_SZSE_ChiNext, df_BSE]
    for df in dfs:
        for row in df.itertuples():
            df = ak.stock_zh_a_hist(
                symbol=row.stock_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"  # 前复权
            )
            print(df)
            break
        
    
# 从文件中获取北交所股票列表
def BSEGetStockListFromFile():

    global tsstocklist
    tsstocklist.clear()
    stocklist = list()

    filename = '标的股票信息.xls'
    workbook = xlrd.open_workbook(filename)

    # 通过索引获取第一个工作表
    sheet = workbook.sheet_by_index(0)

    # 读取单元格数据(测试)
    cell_value = sheet.cell_value(0, 0)  # 读取第一行第一列的数据
    #print(cell_value)

    # 遍历行和列
    bse_nums = 0
    for row_index in range(sheet.nrows):
        if row_index == 0 or row_index == sheet.nrows - 1:
            continue
        cell_value = sheet.cell_value(row_index,0)
        cell_value = cell_value.replace(" ", "")
        stocklist.append(f"{cell_value}.BJ")
        bse_nums = bse_nums + 1

    tsstocklist = stocklist.copy()
    print(f"\n北交所可分析股票数量为{len(tsstocklist)}个\n")

# 从TUSHARE接口获取某只股票日两个月内的日交易数据
def BSEGetDatasFromTushare(stocklist):

    global bse_recommend_stocks

    current_tsstocklist = stocklist
    pro = ts.pro_api()

    # 获取日K数据
    for tsstock in current_tsstocklist:
        print("processing:" + tsstock)
        df = pro.daily(ts_code=tsstock, start_date='20211115', end_date='20251125')

        days_num = len(df)
        if(days_num <20):
            print(f"股票{tsstock}样本空间不足20个")
            continue

        #ts_code = ''
        current_price = average_price5 = average_price10 = average_price20 = 0

        df20 = df.head(20)
        index = 0
        for rows in df20.itertuples(index=False):
            #ts_code = rows.ts_code
            close_price = rows.close

            if(index == 0):
                current_price = close_price

            if(index < 5):
                average_price5 = average_price5 + close_price

            if(index < 10):
                average_price10 = average_price10 + close_price

            average_price20 = average_price20 + close_price

            index = index + 1

        current_price = round(current_price, 2)
        average_price5 = round(average_price5/5, 2)
        average_price10 = round(average_price10/10, 2)
        average_price20 = round(average_price20/20,2)
        
        condition = current_price > average_price5 and average_price5 > average_price10 and average_price10 > average_price20
        if condition:            
            bse_recommend_stocks[tsstock] = [current_price, average_price5, average_price10, average_price20]
            #print(f"股票{tsstock}为短期做多的推荐样本")

# 分析北交所股票
def BSESAnalyzeStocks(market):

    BSEGetStockListFromFile()

    global tsstocklist, bse_recommend_stocks
    bse_recommend_stocks.clear()

    for i in range(6):
        current_tsstocklist = tsstocklist[50*i:50*(i+1)]
        BSEGetDatasFromTushare(current_tsstocklist)         
        time.sleep(60)

    print("\n以下股票为短期做多的推荐样本\n")
    print("股票代码\t参考价格")
    for symbol, price in bse_recommend_stocks.items():
        print(f"{symbol}\t{price}")

# 从Baostock接口获取上交所、深交所股票列表
def HSEGetDatasFromBaostock(market):

    today = date.today()
    specified_trade_date = today.strftime("%Y%m%d")

    global tsstocklist
    tsstocklist.clear()

    pro = ts.pro_api()
    df = pro.daily(trade_date=specified_trade_date)

    sums = sh_nums = sz_nums = bse_nums = 0
    for rows in df.itertuples(index=True):
        tsstocklist.append(rows[1].replace(" ", ""))
        
        if '.SH' in rows[1]:
            sh_nums +=1
        elif '.SZ' in rows[1]:
            sz_nums += 1
        elif '.BJ' in rows[1]:
            bse_nums += 1

        sums += 1

    print(f"通过TUSHARE接口查询到的上交所、深交所、北交所股票数以及总数量分别为{sh_nums}:{sz_nums}:{bse_nums}:{sums}")

# 分析上交所、深交所股票    
def HSESAnalyzeStocksByBaostock(market):

    global bsstocklist, recommend_stocks
    bsstocklist.clear()
    recommend_stocks.clear()
    sh_nums = sz_nums = nums = 0

    try:

        #
        lg = bs.login()

        # 获取证券基本资料
        rs = bs.query_stock_basic()
        
        # 转换为DataFrame
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())        
        df = pd.DataFrame(data_list, columns=rs.fields)

        stock_mark = f"{market['exchage']}.{market['code_prefix']}"
        print(stock_mark)

        for rows in df.itertuples(index=False):
            stock_code = rows[0].replace(" ", "")
            if stock_code.startswith(stock_mark):
                bsstocklist.append(stock_code)
            else:
                continue

        print(f"通过BaoStock接口查询到的待分析股票数量为{len(bsstocklist)}个")

        today = date.today()
        two_months_ago  = today - relativedelta(months=2)
        today = today.strftime("%Y-%m-%d")
        two_months_ago = two_months_ago.strftime("%Y-%m-%d")

        for stock in bsstocklist:
            print("processing:" + stock)
            rs = bs.query_history_k_data_plus(stock,"date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
                start_date=two_months_ago, end_date=today, frequency="d", adjustflag="3")
            #
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            #
            df = pd.DataFrame(data_list, columns=rs.fields)
            days_num = len(df)
            if(days_num <20):
                print(f"股票{stock}样本空间不足20个")
                continue
            df20 = df.tail(20)
            df20 = df20[::-1]

            #
            current_price = average_price5 = average_price10 = average_price20 = 0.0
            index = 0
            for rows in df20.itertuples(index=False):

                close_price = float(rows.close)
                if(index == 0):
                    current_price = close_price
                if(index < 5):
                    average_price5 = average_price5 + close_price
                if(index < 10):
                    average_price10 = average_price10 + close_price
                average_price20 = average_price20 + close_price
                index += 1

            current_price = round(current_price, 2)
            average_price5 = round(average_price5/5, 2)
            average_price10 = round(average_price10/10, 2)
            average_price20 = round(average_price20/20,2)            
            condition = current_price > average_price5 and average_price5 > average_price10 and average_price10 > average_price20
            if(condition):
                recommend_stocks[stock] = [current_price, average_price5, average_price10, average_price20]
                #print(f"股票{stock}为短期做多的推荐样本")

        print("\n以下股票为短期做多的推荐样本\n")
        print("股票代码\t参考价格")
        for symbol, price in recommend_stocks.items():
            print(f"{symbol}\t{price}")

    finally:
        # 
        bs.logout()

# CMD界面1
def stock_market_selector():
    """股票市场选择程序"""
    
    # 显示菜单
    print("=" * 20 + "股票市场分析系统  v1.0" + "=" * 20)
    print("请选择想要分析的股票市场：")
    print("1 - 上证主板")
    print("2 - 上证科创板")
    print("3 - 深证主板")
    print("4 - 深圳创业板")
    print("5 - 北证")
    print("q - 退出程序")
    print("=" * 50)
    
    # 获取用户输入
    while True:
        try:
            #choice = input("请输入选择 (1-5): ").strip()
            choice = input("\n请输入您的选择 (1-5 或 q退出): ").strip().lower()

            if choice == 'q':
                print("! 感谢使用，再见")
                break
            
            # 验证输入
            if choice not in ['1', '2', '3', '4', '5']:
                print("× 输入无效，请重新输入")
                continue
            
            # 根据选择显示对应的市场信息
            markets = {
                '1': '上证主板',
                '2': '上证科创板', 
                '3': '深证主板',
                '4': '深圳创业板',
                '5': '北证'
            }
            
            selected_market = markets[choice]
            
            # 显示选择结果
            print("\n" + "=" * 50)
            print(f"√ 您选择了: {selected_market}")
            print("Hello World!")
            print("=" * 50)
            
            break
            
        except KeyboardInterrupt:
            print("\n\n👋 程序已退出")
            break
        except Exception as e:
            print(f"❌ 发生错误: {e}")
            continue

# CMD界面2
def stock_market_selector2():
  
    def display_menu():
        """显示菜单"""
        print("\n" + "=" * 60)
        print("               股票市场分析系统 v1.0")
        print("=" * 60)
        print("请选择想要分析的股票市场：")
        print()
        for key, market in markets.items():
            print(f"  {key}. {market['name']}")
            #print(f"     {market['description']}")
        print()
        print("  Q. 退出程序")
        print("=" * 60)
    
    def get_user_choice():
        """获取用户选择"""
        while True:
            choice = input("\n请输入您的选择 (1-5 或 Q退出): ").strip().upper()            
            
            if choice == 'Q':
                return None
            elif choice in markets:
                return choice
            else:
                print("❌ 输入无效，请输入 1-5 或 Q")
    
    def process_choice(choice):
        
        # 处理用户选择
        market = markets[choice]

        '''
        print("\n" + "=" * 40)
        print("🎯 选择确认")
        print("=" * 40)
        print(f"市场名称: {market['name']}")
        print(f"市场简称: {market['exchage']}")
        print(f"代码前缀: {market['code_prefix']}")
        print(f"市场描述: {market['description']}")
        '''

        if market['exchage'].upper() in ['SH', 'SZ'] :
            HSESAnalyzeStocksByBaostock(market)

        if market['exchage'].upper() == 'BJ':
            BSESAnalyzeStocks(market) 
        
        # 询问是否继续
        while True:
            continue_choice = input("\n是否继续选择其他市场? (Y/N): ").strip().upper()
            if continue_choice in ['Y', 'N']:
                return continue_choice == 'Y'
            else:
                print("❌ 请输入 Y 或 N")
    
    # 主程序循环
    print("🚀 启动股票市场分析系统...")
    
    while True:
        try:
            display_menu()
            choice = get_user_choice()
            
            if choice is None:
                print("\n👋 感谢使用，再见！")
                break
            
            should_continue = process_choice(choice)
            
            if not should_continue:
                print("\n👋 感谢使用，再见！")
                break
                
        except KeyboardInterrupt:
            print("\n\n⚠️  检测到中断信号，程序退出")
            break
        except Exception as e:
            print(f"\n❌ 程序出错: {e}")
            retry = input("是否重新尝试? (Y/N): ").strip().upper()
            if retry != 'Y':
                break

def main():

    GetStockListByAKShare()

    GetStockDataByAKShare()
    
    #stock_market_selector2()

if __name__ == "__main__":
    main()