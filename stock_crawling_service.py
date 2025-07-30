import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict
from postgres_db import PostgresDB

try:
    import FinanceDataReader as fdr
except ImportError:
    fdr = None

class StockCrawlingService:
    def __init__(self):
        self.postgres_db = PostgresDB()
    
    def get_kospi_stock_list(self) -> List[Dict[str, str]]:
        sample_stocks = [
            {"code": "005930", "name": "삼성전자"},
            {"code": "000660", "name": "SK하이닉스"},
            {"code": "035420", "name": "NAVER"},
            {"code": "051910", "name": "LG화학"},
            {"code": "006400", "name": "삼성SDI"},
            {"code": "035720", "name": "카카오"},
            {"code": "207940", "name": "삼성바이오로직스"},
            {"code": "068270", "name": "셀트리온"},
            {"code": "028260", "name": "삼성물산"},
            {"code": "066570", "name": "LG전자"}
        ]
        return sample_stocks
    
    def get_stock_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            if fdr is None:
                return pd.DataFrame()
            
            data = fdr.DataReader(stock_code, start_date, end_date)
            if data.empty:
                return pd.DataFrame()
            
            data = data.reset_index()
            data['stock_code'] = stock_code
            data['date'] = data['Date'].dt.date
            data['open_price'] = (data['Open']).astype(int)
            data['high_price'] = (data['High']).astype(int)
            data['low_price'] = (data['Low']).astype(int)
            data['close_price'] = (data['Close']).astype(int)
            data['volume'] = data['Volume'].astype(int)
            
            return data[['stock_code', 'date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']]
        except Exception as e:
            print(f"주식 데이터 가져오기 오류 ({stock_code}): {e}")
            return pd.DataFrame()
    
    def save_stock_info(self, stocks: List[Dict[str, str]]) -> int:
        try:
            print(f"Saving {len(stocks)} stock info records to PostgreSQL")
            saved_count = 0
            for stock in stocks:
                check_query = "SELECT COUNT(*) FROM stock_info WHERE code = :code"
                exists = self.postgres_db.execute_scalar(check_query, {"code": stock["code"]})
                
                if exists == 0:
                    query = """
                    INSERT INTO stock_info (code, name, market, created_at, updated_at) 
                    VALUES (:code, :name, :market, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """
                else:
                    query = """
                    UPDATE stock_info SET 
                        name = :name,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE code = :code
                    """
                
                params = {"code": stock["code"], "name": stock["name"], "market": "KOSPI"}
                result = self.postgres_db.execute_update(query, params)
                print(f"Stock info saved: {stock['name']} ({stock['code']}) - affected rows: {result}")
                saved_count += 1
            
            print(f"Total stock info saved: {saved_count}")
            return saved_count
        except Exception as e:
            print(f"종목 정보 저장 오류: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def save_stock_daily_data(self, df: pd.DataFrame) -> int:
        try:
            if df.empty:
                print("No daily data to save (empty DataFrame)")
                return 0
            
            print(f"Saving {len(df)} daily data records to PostgreSQL")
            saved_count = 0
            for _, row in df.iterrows():
                check_query = "SELECT COUNT(*) FROM stock_daily_data WHERE stock_code = :stock_code AND date = :date"
                exists = self.postgres_db.execute_scalar(check_query, {"stock_code": row['stock_code'], "date": row['date']})
                
                if exists == 0:
                    query = """
                    INSERT INTO stock_daily_data (stock_code, date, open_price, high_price, low_price, close_price, volume, created_at)
                    VALUES (:stock_code, :date, :open_price, :high_price, :low_price, :close_price, :volume, CURRENT_TIMESTAMP)
                    """
                else:
                    query = """
                    UPDATE stock_daily_data SET 
                        open_price = :open_price,
                        high_price = :high_price,
                        low_price = :low_price,
                        close_price = :close_price,
                        volume = :volume
                    WHERE stock_code = :stock_code AND date = :date
                    """
                
                params = {
                    "stock_code": row['stock_code'],
                    "date": row['date'],
                    "open_price": row['open_price'],
                    "high_price": row['high_price'],
                    "low_price": row['low_price'],
                    "close_price": row['close_price'],
                    "volume": row['volume']
                }
                
                result = self.postgres_db.execute_update(query, params)
                if saved_count < 3:  # 처음 3개만 로그 출력
                    print(f"Daily data saved: {row['stock_code']} {row['date']} - affected rows: {result}")
                saved_count += 1
            
            print(f"Total daily data saved: {saved_count}")
            return saved_count
        except Exception as e:
            print(f"일별 데이터 저장 오류: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def crawl_all_stocks(self, years: int = 5) -> Dict[str, int]:
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=years * 365)
            
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            stocks = self.get_kospi_stock_list()
            if not stocks:
                return {"error": "종목 리스트를 가져올 수 없습니다."}
            
            stock_info_count = self.save_stock_info(stocks)
            total_data_count = 0
            processed_stocks = 0
            
            for stock in stocks:
                stock_code = stock["code"]
                stock_name = stock["name"]
                
                df = self.get_stock_data(stock_code, start_date_str, end_date_str)
                
                if not df.empty:
                    data_count = self.save_stock_daily_data(df)
                    total_data_count += data_count
                
                processed_stocks += 1
            
            return {
                "processed_stocks": processed_stocks,
                "saved_stock_info": stock_info_count,
                "saved_daily_data": total_data_count,
                "period": f"{start_date_str} ~ {end_date_str}"
            }
        except Exception as e:
            return {"error": str(e)}
    
    def crawl_company_data(self) -> Dict[str, any]:
        try:
            from OpenDartReader.dart import OpenDartReader
            
            api_key = '34f3abfbcd2fc0ce30b09f05466a98c1aa5fb865'
            dart = OpenDartReader(api_key)
            
            stock_codes = ['005930', '000660', '035420', '051910', '006400', 
                          '035720', '207940', '068270', '028260', '066570']
            
            saved_companies = 0
            saved_financial = 0
            
            for stock_code in stock_codes:
                try:
                    company_info = dart.company(stock_code)
                    
                    if company_info is not None:
                        saved_companies += self._save_company_info_dict(company_info)
                        
                        corp_code = company_info.get('corp_code')
                        if corp_code:
                            for year in ['2023', '2022']:
                                try:
                                    result = dart.finstate_all(corp_code, year)
                                    if result is not None and hasattr(result, 'empty') and not result.empty:
                                        saved_count = self._save_financial_data(corp_code, year, result)
                                        saved_financial += saved_count
                                except Exception as year_error:
                                    print(f"{stock_code} {year}년 오류: {year_error}")
                except Exception as e:
                    print(f"{stock_code} 처리 중 오류: {e}")
            
            return {
                "message": "기업 정보 수집 완료",
                "saved_companies": saved_companies,
                "saved_financial_data": saved_financial
            }
        except Exception as e:
            return {"error": str(e)}
    
    def _save_company_info_dict(self, company_dict) -> int:
        try:
            query = """
            INSERT INTO company_info (
                corp_code, corp_name, corp_name_eng, stock_name, stock_code,
                ceo_nm, corp_cls, jurir_no, bizr_no, adres, hm_url,
                ir_url, phn_no, fax_no, induty_code, est_dt, acc_mt,
                created_at, updated_at
            ) VALUES (
                :corp_code, :corp_name, :corp_name_eng, :stock_name, :stock_code,
                :ceo_nm, :corp_cls, :jurir_no, :bizr_no, :adres, :hm_url,
                :ir_url, :phn_no, :fax_no, :induty_code, :est_dt, :acc_mt,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            ON CONFLICT (corp_code) DO UPDATE SET
                corp_name = EXCLUDED.corp_name,
                stock_name = EXCLUDED.stock_name,
                ceo_nm = EXCLUDED.ceo_nm,
                induty_code = EXCLUDED.induty_code,
                updated_at = CURRENT_TIMESTAMP
            """
            
            params = {
                "corp_code": company_dict.get('corp_code'),
                "corp_name": company_dict.get('corp_name'),
                "corp_name_eng": company_dict.get('corp_name_eng'),
                "stock_name": company_dict.get('stock_name'),
                "stock_code": company_dict.get('stock_code'),
                "ceo_nm": company_dict.get('ceo_nm'),
                "corp_cls": company_dict.get('corp_cls'),
                "jurir_no": company_dict.get('jurir_no'),
                "bizr_no": company_dict.get('bizr_no'),
                "adres": company_dict.get('adres'),
                "hm_url": company_dict.get('hm_url'),
                "ir_url": company_dict.get('ir_url'),
                "phn_no": company_dict.get('phn_no'),
                "fax_no": company_dict.get('fax_no'),
                "induty_code": company_dict.get('induty_code'),
                "est_dt": company_dict.get('est_dt'),
                "acc_mt": company_dict.get('acc_mt')
            }
            
            self.postgres_db.execute_update(query, params)
            return 1
        except Exception as e:
            print(f"기업 정보 저장 오류: {e}")
            return 0
    
    def _save_financial_data(self, corp_code: str, year: str, fs_data) -> int:
        try:
            saved_count = 0
            
            for idx, row in fs_data.iterrows():
                try:
                    thstrm_amount = None
                    frmtrm_amount = None
                    
                    if row.get('thstrm_amount'):
                        try:
                            thstrm_amount = int(str(row.get('thstrm_amount')).replace(',', '').replace('-', '0'))
                        except:
                            thstrm_amount = None
                    
                    if row.get('frmtrm_amount'):
                        try:
                            frmtrm_amount = int(str(row.get('frmtrm_amount')).replace(',', '').replace('-', '0'))
                        except:
                            frmtrm_amount = None
                    
                    query = """
                    INSERT INTO financial_data (
                        corp_code, bsns_year, reprt_code, account_nm, fs_div,
                        fs_nm, sj_div, sj_nm, thstrm_nm, thstrm_amount,
                        frmtrm_nm, frmtrm_amount, ord, currency, created_at
                    ) VALUES (
                        :corp_code, :bsns_year, :reprt_code, :account_nm, :fs_div,
                        :fs_nm, :sj_div, :sj_nm, :thstrm_nm, :thstrm_amount,
                        :frmtrm_nm, :frmtrm_amount, :ord, :currency, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (corp_code, bsns_year, reprt_code, account_nm, sj_div) 
                    DO UPDATE SET
                        thstrm_amount = EXCLUDED.thstrm_amount,
                        frmtrm_amount = EXCLUDED.frmtrm_amount,
                        updated_at = CURRENT_TIMESTAMP
                    """
                    
                    def truncate_string(value, max_length):
                        if value is None:
                            return None
                        return str(value)[:max_length] if len(str(value)) > max_length else str(value)
                    
                    params = {
                        "corp_code": corp_code,
                        "bsns_year": year,
                        "reprt_code": truncate_string(row.get('reprt_code'), 5),
                        "account_nm": truncate_string(row.get('account_nm'), 200),
                        "fs_div": truncate_string(row.get('fs_div'), 2),
                        "fs_nm": truncate_string(row.get('fs_nm'), 100),
                        "sj_div": truncate_string(row.get('sj_div'), 2),
                        "sj_nm": truncate_string(row.get('sj_nm'), 100),
                        "thstrm_nm": truncate_string(row.get('thstrm_nm'), 20),
                        "thstrm_amount": thstrm_amount,
                        "frmtrm_nm": truncate_string(row.get('frmtrm_nm'), 20),
                        "frmtrm_amount": frmtrm_amount,
                        "ord": row.get('ord'),
                        "currency": truncate_string(row.get('currency'), 10)
                    }
                    
                    self.postgres_db.execute_update(query, params)
                    saved_count += 1
                except Exception as row_error:
                    continue
            
            return saved_count
        except Exception as e:
            print(f"재무 데이터 저장 오류: {e}")
            return 0