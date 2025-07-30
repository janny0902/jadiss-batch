import pymysql
import os
from datetime import datetime, date

class BatchDB:
    def __init__(self):
        self.config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 3309)),
            'user': os.getenv('DB_USER', 'batch_user'),
            'password': os.getenv('DB_PASSWORD', 'batch123'),
            'database': os.getenv('DB_NAME', 'jadiss_batch'),
            'charset': 'utf8mb4'
        }
    
    def get_connection(self):
        return pymysql.connect(**self.config)
    
    def get_job_by_name(self, job_name):
        with self.get_connection() as conn:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute("SELECT * FROM batch_jobs WHERE job_name = %s", (job_name,))
            result = cursor.fetchone()
            if not result:
                print(f"Job not found: {job_name}")
                # 사용 가능한 작업 목록 출력
                cursor.execute("SELECT job_name FROM batch_jobs")
                available_jobs = [row['job_name'] for row in cursor.fetchall()]
                print(f"Available jobs: {available_jobs}")
            return result
    
    def get_all_jobs(self):
        with self.get_connection() as conn:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute("SELECT * FROM batch_jobs WHERE is_active = 1")
            return cursor.fetchall()
    
    def create_execution(self, job_id, execution_id):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO batch_executions (job_id, execution_id) VALUES (%s, %s)",
                (job_id, execution_id)
            )
            conn.commit()
    
    def update_execution(self, execution_id, status, error_message=None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """UPDATE batch_executions 
                   SET status = %s, end_time = %s, error_message = %s,
                       duration_ms = TIMESTAMPDIFF(MICROSECOND, start_time, %s) / 1000
                   WHERE execution_id = %s""",
                (status, datetime.now(), error_message, datetime.now(), execution_id)
            )
            conn.commit()
    
    def get_execution(self, execution_id):
        with self.get_connection() as conn:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute("SELECT * FROM batch_executions WHERE execution_id = %s", (execution_id,))
            return cursor.fetchone()
    
    def add_log(self, execution_id, level, message):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO batch_logs (execution_id, log_level, message) VALUES (%s, %s, %s)",
                (execution_id, level, message)
            )
            conn.commit()
    
    def get_logs(self, execution_id):
        with self.get_connection() as conn:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(
                "SELECT * FROM batch_logs WHERE execution_id = %s ORDER BY created_at",
                (execution_id,)
            )
            return cursor.fetchall()
    
    def get_batch_stats(self):
        with self.get_connection() as conn:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute("""
                SELECT j.job_name, 
                       COUNT(CASE WHEN e.status = 'SUCCESS' THEN 1 END) as success_count,
                       COUNT(CASE WHEN e.status = 'FAILED' THEN 1 END) as error_count,
                       AVG(e.duration_ms) as avg_duration
                FROM batch_jobs j
                LEFT JOIN batch_executions e ON j.id = e.job_id
                WHERE DATE(e.start_time) = CURDATE() OR e.start_time IS NULL
                GROUP BY j.id, j.job_name
            """)
            return cursor.fetchall()
    
    def update_job_cron(self, job_id, cron_expression):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE batch_jobs SET cron_expression = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                (cron_expression, job_id)
            )
            conn.commit()
            return cursor.rowcount