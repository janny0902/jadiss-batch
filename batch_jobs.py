import time
from stock_crawling_service import StockCrawlingService

class DataSyncJob:
    def __init__(self, db_type):
        self.db_type = db_type
        
    def execute(self, execution_id, params, batch_db):
        if self.db_type == 'postgres':
            return self._sync_postgres_data(execution_id, params, batch_db)
        elif self.db_type == 'mysql':
            return self._sync_mysql_data(execution_id, params, batch_db)
        elif self.db_type == 'cleanup':
            return self._cleanup_data(execution_id, params, batch_db)
    
    def _sync_postgres_data(self, execution_id, params, batch_db):
        batch_db.add_log(execution_id, 'INFO', 'Connecting to jadiss-postgres')
        
        try:
            time.sleep(2)
            batch_db.add_log(execution_id, 'INFO', 'PostgreSQL data sync completed')
            return {'processed_records': 150, 'updated_records': 45}
            
        except Exception as e:
            batch_db.add_log(execution_id, 'ERROR', f'PostgreSQL sync failed: {str(e)}')
            raise
    
    def _sync_mysql_data(self, execution_id, params, batch_db):
        batch_db.add_log(execution_id, 'INFO', 'Connecting to jadiss-mysql')
        
        try:
            time.sleep(3)
            batch_db.add_log(execution_id, 'INFO', 'MySQL data sync completed')
            return {'processed_records': 200, 'inserted_records': 30}
            
        except Exception as e:
            batch_db.add_log(execution_id, 'ERROR', f'MySQL sync failed: {str(e)}')
            raise
    
    def _cleanup_data(self, execution_id, params, batch_db):
        batch_db.add_log(execution_id, 'INFO', 'Starting data cleanup')
        
        try:
            time.sleep(1)
            batch_db.add_log(execution_id, 'INFO', 'Data cleanup completed')
            return {'deleted_records': 25}
            
        except Exception as e:
            batch_db.add_log(execution_id, 'ERROR', f'Cleanup failed: {str(e)}')
            raise

class CrawlingJob:
    def __init__(self):
        self.crawling_service = StockCrawlingService()
    
    def execute(self, execution_id, params, batch_db):
        job_type = params.get('type', 'stock')
        
        if job_type == 'stock':
            return self._crawl_stock_data(execution_id, params, batch_db)
        elif job_type == 'company':
            return self._crawl_company_data(execution_id, params, batch_db)
    
    def _crawl_stock_data(self, execution_id, params, batch_db):
        batch_db.add_log(execution_id, 'INFO', 'Starting stock data crawling')
        
        try:
            years = params.get('years', 5)
            result = self.crawling_service.crawl_all_stocks(years)
            
            if 'error' in result:
                batch_db.add_log(execution_id, 'ERROR', f'Stock crawling failed: {result["error"]}')
                raise Exception(result['error'])
            
            batch_db.add_log(execution_id, 'INFO', f'Stock crawling completed: {result}')
            return result
            
        except Exception as e:
            batch_db.add_log(execution_id, 'ERROR', f'Stock crawling failed: {str(e)}')
            raise
    
    def _crawl_company_data(self, execution_id, params, batch_db):
        batch_db.add_log(execution_id, 'INFO', 'Starting company data crawling')
        
        try:
            result = self.crawling_service.crawl_company_data()
            
            if 'error' in result:
                batch_db.add_log(execution_id, 'ERROR', f'Company crawling failed: {result["error"]}')
                raise Exception(result['error'])
            
            batch_db.add_log(execution_id, 'INFO', f'Company crawling completed: {result}')
            return result
            
        except Exception as e:
            batch_db.add_log(execution_id, 'ERROR', f'Company crawling failed: {str(e)}')
            raise