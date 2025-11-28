# stock_data_manager.py

import akshare as ak
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import numpy as np
import logging

class StockDataManager():
    def __init__(self, db_manager):
        self.db = db_manager

        self.driver = 'pymysql'
        connection_string = f"mysql+{self.driver}://{self.db.user}:{self.db.password}@{self.db.host}:{self.db.port}/{self.db.database}"
        self.engine = create_engine(
                connection_string,
                echo=False,     # 设置为True可以查看SQL语句
                pool_recycle=3600,
                pool_pre_ping=True,
                connect_args={'connect_timeout': 10}
            )

        self.df_SSE_Main = pd.DataFrame()
        self.df_SSE_STAR = pd.DataFrame()
        self.df_SZSE_Main = pd.DataFrame()
        self.df_SZSE_ChiNext = pd.DataFrame()
        self.df_BSE = pd.DataFrame()
        self.df_all_stocks = pd.DataFrame()
        self.df_all_stocks_data = pd.DataFrame()
        self.logger = logging.getLogger(__name__)

    def get_all_stock_list(self):

        try:
            

            # 获取上交所股票-主板
            df_SSE_Main = ak.stock_info_sh_name_code(symbol="主板A股")
            # 重命名列以便统一处理
            df_SSE_Main = df_SSE_Main.rename(columns={
                '证券代码': 'stock_code',
                '证券简称': 'stock_name',
                '上市日期': 'listing_date'
            })
            # 格式化上市日期 
            if not pd.api.types.is_datetime64_any_dtype(df_SSE_Main['listing_date']):
                df_SSE_Main['listing_date'] = pd.to_datetime(df_SSE_Main['listing_date'])
            # 格式化为年月日
            df_SSE_Main['listing_date'] = df_SSE_Main['listing_date'].dt.strftime('%Y-%m-%d')

            df_SSE_Main['exchange'] = 'sh'
            df_SSE_Main['market'] = 'SSE'
            df_SSE_Main['market_type'] = '主板A股'
            df_SSE_Main['stock_code_full'] = df_SSE_Main['stock_code'] + '.SH'
            print(f"获取到 {len(df_SSE_Main)} 只上交所主板A股股票")



            # 获取上交所股票-科创板
            df_SSE_STAR = ak.stock_info_sh_name_code(symbol="科创板")
            # 重命名列以便统一处理
            df_SSE_STAR = df_SSE_STAR.rename(columns={
                '证券代码': 'stock_code',
                '证券简称': 'stock_name',
                '上市日期': 'listing_date'
            })
            df_SSE_STAR['listing_date'] = pd.to_datetime(df_SSE_STAR['listing_date'])
            df_SSE_STAR['listing_date'] = df_SSE_STAR['listing_date'].dt.strftime('%Y-%m-%d')
            df_SSE_STAR['exchange'] = 'sh'
            df_SSE_STAR['market'] = 'SSE'
            df_SSE_STAR['market_type'] = '科创板'
            df_SSE_STAR['stock_code_full'] = df_SSE_STAR['stock_code'] + '.SH'
            print(f"获取到 {len(df_SSE_STAR):4} 只科创板股票")



            # 获取深交所股票
            stock_info_sz_df = ak.stock_info_sz_name_code(symbol="A股列表")
            # 重命名列以便统一处理
            stock_info_sz_df = stock_info_sz_df.rename(columns={
                'A股代码': 'stock_code',
                'A股简称': 'stock_name',
                'A股上市日期': 'listing_date',
                '板块': 'market_type'
            })
            stock_info_sz_df['exchange'] = 'sz'
            stock_info_sz_df['market'] = 'SZSE'    
            stock_info_sz_df['stock_code_full'] = stock_info_sz_df['stock_code'] + '.SZ'
            # 分别提取主板和创业板
            condition = stock_info_sz_df['stock_code'].str.startswith('00')
            df_SZSE_Main = stock_info_sz_df[condition].copy()
            print(f"获取到 {len(df_SZSE_Main)} 只深交所主板A股股票")
            condition = stock_info_sz_df['stock_code'].str.startswith('30')
            df_SZSE_ChiNext = stock_info_sz_df[condition].copy()
            print(f"获取到 {len(df_SZSE_ChiNext)} 只创业板股票")

            


            # 获取北交所股票
            df_BSE = ak.stock_info_bj_name_code()
            # 重命名列以便统一处理
            df_BSE = df_BSE.rename(columns={
                '证券代码': 'stock_code',
                '证券简称': 'stock_name',
                '上市日期': 'listing_date'
            })
            df_BSE['listing_date'] = pd.to_datetime(df_BSE['listing_date'])
            df_BSE['listing_date'] = df_BSE['listing_date'].dt.strftime('%Y-%m-%d')
            df_BSE['exchange'] = 'bj'
            df_BSE['market'] = 'BSE' 
            df_BSE['market_type'] = ''
            df_BSE['stock_code_full'] = df_BSE['stock_code'] + '.BJ'
            print(f"获取到 {len(df_BSE):4} 只北交所股票")


            # 纵向合并all_stocks
            df1 = df_SSE_Main[['stock_code', 'stock_name', 'exchange', 'market', 'market_type', 'listing_date']]
            df2 = df_SSE_STAR[['stock_code', 'stock_name', 'exchange', 'market', 'market_type', 'listing_date']]
            df3 = df_SZSE_Main[['stock_code', 'stock_name', 'exchange', 'market', 'market_type', 'listing_date']]
            df4 = df_SZSE_ChiNext[['stock_code', 'stock_name', 'exchange', 'market', 'market_type', 'listing_date']]
            df5 = df_BSE[['stock_code', 'stock_name', 'exchange', 'market', 'market_type', 'listing_date']]

            self.df_SSE_Main = df1
            self.df_SSE_STAR = df2
            self.df_SZSE_Main = df3
            self.df_SZSE_ChiNext = df4
            self.df_BSE = df5
            self.df_all_stocks = pd.concat([df1, df2, df3, df4, df5], ignore_index=True)            
            return self.df_all_stocks
            
        except Exception as e:
            print(f"获取股票列表失败: {e}")
            return pd.DataFrame()
    
    def update_stock_basic_info(self):
        """
        更新股票基本信息到数据库
        """
        try:
            stock_list = self.get_all_stock_list()
            if not stock_list.empty:
                stock_list.to_sql(
                    'stock_basic', 
                    self.engine, 
                    if_exists='append', 
                    index=False,
                    method='multi'
                )
                print("股票基本信息更新完成")
            else:
                print("未获取到股票列表数据")
        except Exception as e:
            print(f"更新股票基本信息失败: {e}")
    
    def get_stock_daily_data(self, start_date=None, end_date=None):

        if start_date is None:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        end_date = "20251130"
        
        try:
            stock_list = self.get_all_stock_list()
            #
            start_date = "20091030" # 创业板开市时间
            stock_list = self.df_SZSE_ChiNext
            #start_date = "20190722" # 科创板开市时间
            #stock_list = self.df_SSE_STAR
            #start_date = "20211125" # 北交所开市时间
            #stock_list = self.df_BSE
            
            for row in stock_list.itertuples():
                if int(row.stock_code) < 300430:
                    continue
                symbol = f"{row.exchange}{row.stock_code}".lower()
                df_daily_data = ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust="qfq")
                df_daily_data = df_daily_data.rename(columns={
                    'date': 'trade_date',
                    'open': 'open_price',
                    'high': 'high_price',
                    'low':'low_price',
                    'close':'close_price',
                    'turnover':'turnover_rate'
                })
                df_daily_data['stock_code'] = row.stock_code
                df_daily_data['previous_close_price'] = 0.0
                df_daily_data = df_daily_data[['stock_code', 'trade_date', 'open_price', 'high_price', 'low_price', 'close_price', 'previous_close_price', 'volume', 'turnover_rate']]
                df_daily_data.loc[1:, 'previous_close_price'] = df_daily_data['close_price'].shift(1).loc[1:]

                #
                if df_daily_data.empty:
                    continue

                #
                df_daily_data.to_sql(
                    'stock_daily', 
                    self.engine, 
                    if_exists='append', 
                    index=False,
                    chunksize=5000,
                    method='multi'
                )
                print(f"{row.stock_code}入库完成:{len(df_daily_data)}")

                #
                self.df_all_stocks_data = pd.concat([self.df_all_stocks_data, df_daily_data], ignore_index=True) 
                
            #
            '''

            if not self.df_all_stocks_data.empty:
                self.df_all_stocks_data.to_sql(
                    'stock_daily', 
                    self.engine, 
                    if_exists='append', 
                    index=False,
                    chunksize=5000,
                    method='multi'
                )
                print("股票日交易数据更新完成")
            else:
                print("未获取到股票日交易数据")
            '''


            return self.df_all_stocks_data
                
        except Exception as e:
            print(f"更新股票日交易数据失败: {e}")
            return pd.DataFrame()
    
    def update_all_stocks_daily_data(self, days=30, batch_size=50):
        """
        批量更新所有股票的日交易数据
        days: 获取最近多少天的数据
        batch_size: 每批处理的股票数量
        """
        try:
            # 获取股票列表
            stock_list = self.get_all_stock_list()
            total_stocks = len(stock_list)
            print(f"开始更新 {total_stocks} 只股票的最近 {days} 天数据...")
            
            # 计算日期范围
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
            
            success_count = 0
            failed_stocks = []
            
            for i in range(0, total_stocks, batch_size):
                batch = stock_list.iloc[i:i+batch_size]
                batch_data = []
                
                for _, stock in batch.iterrows():
                    try:
                        stock_data = self.get_stock_daily_data(
                            stock['stock_code'], 
                            stock['market'],
                            start_date, 
                            end_date
                        )
                        if not stock_data.empty:
                            batch_data.append(stock_data)
                            success_count += 1
                        else:
                            failed_stocks.append(stock['stock_code'])
                        
                        # 添加延迟避免请求过快
                        time.sleep(0.1)
                        
                    except Exception as e:
                        print(f"处理股票 {stock['stock_code']} 时出错: {e}")
                        failed_stocks.append(stock['stock_code'])
                
                # 批量保存数据
                if batch_data:
                    combined_data = pd.concat(batch_data, ignore_index=True)
                    combined_data['trade_date'] = pd.to_datetime(combined_data['trade_date'])
                    
                    # 保存到数据库
                    combined_data.to_sql(
                        'stock_daily', 
                        self.db.engine, 
                        if_exists='append', 
                        index=False,
                        method='multi'
                    )
                
                print(f"进度: {min(i+batch_size, total_stocks)}/{total_stocks}")
            
            print(f"数据更新完成! 成功: {success_count}, 失败: {len(failed_stocks)}")
            if failed_stocks:
                print(f"失败的股票: {failed_stocks[:10]}")  # 只显示前10个
            
        except Exception as e:
            print(f"批量更新数据失败: {e}")

