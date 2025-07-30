import os
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from typing import List, Dict, Any, Optional, Union

class PostgresDB:
    def __init__(self):
        self.config = {
            'host': os.getenv('POSTGRES_HOST', 'host.docker.internal'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'user': os.getenv('POSTGRES_USER', 'root'),
            'password': os.getenv('POSTGRES_PASSWORD', 'jadiss01##'),
            'database': os.getenv('POSTGRES_DB', 'core')
        }
        print(f"PostgreSQL config: {self.config}")
        
        # SQLAlchemy 엔진 생성
        db_url = f"postgresql://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}"
        self.engine = create_engine(db_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # 연결 테스트
        try:
            with self.SessionLocal() as session:
                result = session.execute(text("SELECT 1"))
                print("PostgreSQL connection successful")
        except Exception as e:
            print(f"PostgreSQL connection failed: {e}")
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        with self.SessionLocal() as session:
            result = session.execute(text(query), params or {})
            return [dict(row._mapping) for row in result]
    
    def execute_update(self, query: str, params: Optional[Dict[str, Any]] = None) -> int:
        with self.SessionLocal() as session:
            result = session.execute(text(query), params or {})
            session.commit()
            return result.rowcount
    
    def execute_scalar(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        with self.SessionLocal() as session:
            result = session.execute(text(query), params or {})
            first_row = result.first()
            return first_row[0] if first_row else None