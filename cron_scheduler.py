import asyncio
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from database import BatchDB
from batch_executor import BatchExecutor
import uuid

class CronSchedulerService:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.db = BatchDB()
        self.executor = BatchExecutor(self.db)
        self.is_running = False
    
    def start(self):
        if self.is_running:
            return
        
        # 활성화된 배치 작업들을 스케줄에 등록
        jobs = self.db.get_all_jobs()
        
        for job in jobs:
            if job['is_active'] and job['cron_expression']:
                try:
                    # Cron 표현식을 파싱하여 스케줄 등록
                    cron_parts = job['cron_expression'].split()
                    if len(cron_parts) == 6:  # 초 분 시 일 월 요일
                        trigger = CronTrigger(
                            second=cron_parts[0],
                            minute=cron_parts[1], 
                            hour=cron_parts[2],
                            day=cron_parts[3],
                            month=cron_parts[4],
                            day_of_week=cron_parts[5]
                        )
                        
                        self.scheduler.add_job(
                            func=self._execute_scheduled_job,
                            trigger=trigger,
                            args=[job['job_name']],
                            id=f"job_{job['id']}",
                            name=job['job_name']
                        )
                        
                        print(f"Scheduled job: {job['job_name']} with cron: {job['cron_expression']}")
                except Exception as e:
                    print(f"Failed to schedule job {job['job_name']}: {e}")
        
        self.scheduler.start()
        self.is_running = True
        print("Cron scheduler started")
    
    def stop(self):
        if self.is_running:
            self.scheduler.shutdown()
            self.is_running = False
            print("Cron scheduler stopped")
    
    def _execute_scheduled_job(self, job_name):
        """스케줄된 작업 실행"""
        try:
            execution_id = str(uuid.uuid4())
            
            # 작업 정보 조회
            job = self.db.get_job_by_name(job_name)
            if not job:
                print(f"Job not found: {job_name}")
                return
            
            # 실행 기록 생성
            self.db.create_execution(job['id'], execution_id)
            
            # 크롤링 작업별 파라미터 설정
            params = {}
            if job_name == 'crawling':
                params = {'type': 'stock', 'years': 5}
            elif job_name == 'company_crawling':
                params = {'type': 'company'}
            
            # 백그라운드에서 작업 실행
            thread = threading.Thread(
                target=self.executor.execute_job,
                args=(execution_id, job_name, params)
            )
            thread.start()
            
            print(f"Scheduled job executed: {job_name} (ID: {execution_id})")
            
        except Exception as e:
            print(f"Failed to execute scheduled job {job_name}: {e}")

# 전역 스케줄러 인스턴스
cron_scheduler = CronSchedulerService()