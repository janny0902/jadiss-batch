# Jadiss Batch API

FastAPI 기반 배치 작업 실행 및 모니터링 API

## 환경 구성

### 필수 요구사항
- Python 3.10
- Docker & Docker Compose
- PostgreSQL (jadiss-postgres)
- MySQL (jadiss-batch-mysql)

### 환경 변수
```bash
DB_HOST=host.docker.internal
DB_PORT=3309
DB_USER=batch_user
DB_PASSWORD=batch123
DB_NAME=jadiss_batch

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_DB=jadiss
```

## 설치 및 실행

### Docker로 실행 (권장)
```bash
# 배치 API 실행
docker-compose up -d --build

# 로그 확인
docker-compose logs -f
```

### 로컬 실행
```bash
# 의존성 설치
pip install -r requirements.txt

# 서버 실행
uvicorn main:app --host 0.0.0.0 --port 8092 --reload
```

## API 엔드포인트

### 배치 실행
```bash
POST /batch/execute
{
  "job_name": "crawling",
  "params": {"type": "stock", "years": 5}
}
```

### 상태 조회
```bash
GET /batch/status/{execution_id}
GET /batch/logs/{execution_id}
GET /batch/jobs
GET /batch/stats
```

## 배치 작업 종류

- **data_sync_postgres**: PostgreSQL 데이터 동기화
- **data_sync_mysql**: MySQL 데이터 동기화
- **data_cleanup**: 데이터 정리 작업
- **crawling**: 주식 데이터 크롤링
- **company_crawling**: 기업 정보 크롤링

## 접속 정보
- API: http://localhost:8092
- API 문서: http://localhost:8092/docs