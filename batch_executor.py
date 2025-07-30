import time
import traceback
from datetime import datetime
from batch_jobs import DataSyncJob, CrawlingJob

class BatchExecutor:
    def __init__(self, db):
        self.db = db
        self.jobs = {
            'data_sync_postgres': DataSyncJob('postgres'),
            'data_sync_mysql': DataSyncJob('mysql'),
            'data_cleanup': DataSyncJob('cleanup'),
            'crawling': CrawlingJob(),
            'company_crawling': CrawlingJob()
        }
    
    def execute_job(self, execution_id, job_name, params):
        try:
            print(f"Executing job: {job_name} with params: {params}")
            print(f"Available jobs: {list(self.jobs.keys())}")
            
            self.db.add_log(execution_id, 'INFO', f'Starting job: {job_name}')
            
            if job_name not in self.jobs:
                available_jobs = list(self.jobs.keys())
                error_msg = f'Unknown job: {job_name}. Available jobs: {available_jobs}'
                print(error_msg)
                raise Exception(error_msg)
            
            job = self.jobs[job_name]
            result = job.execute(execution_id, params, self.db)
            
            self.db.update_execution(execution_id, 'SUCCESS')
            self.db.add_log(execution_id, 'INFO', f'Job completed successfully: {result}')
            
        except Exception as e:
            error_msg = str(e)
            print(f"Job execution failed: {error_msg}")
            self.db.update_execution(execution_id, 'FAILED', error_msg)
            self.db.add_log(execution_id, 'ERROR', f'Job failed: {error_msg}')
            self.db.add_log(execution_id, 'ERROR', traceback.format_exc())