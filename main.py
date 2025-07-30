from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
import threading
import uuid
from datetime import datetime
from database import BatchDB
from batch_executor import BatchExecutor

app = FastAPI(title="Jadiss Batch API", version="1.0.0")

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3001", "http://127.0.0.1:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
db = BatchDB()
executor = BatchExecutor(db)

class BatchRequest(BaseModel):
    job_name: str
    params: dict = {}

class BatchResponse(BaseModel):
    execution_id: str
    status: str
    message: str

@app.post("/batch/execute", response_model=BatchResponse)
async def execute_batch(request: BatchRequest):
    try:
        execution_id = str(uuid.uuid4())
        
        # 배치 실행 기록 생성
        job = db.get_job_by_name(request.job_name)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        db.create_execution(job['id'], execution_id)
        
        # 백그라운드에서 배치 실행
        thread = threading.Thread(
            target=executor.execute_job,
            args=(execution_id, request.job_name, request.params)
        )
        thread.daemon = True
        thread.start()
        
        return BatchResponse(
            execution_id=execution_id,
            status="RUNNING",
            message="Batch job started successfully"
        )
    except HTTPException:
        raise
    except Exception as e:
        print(f"Batch execution error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/batch/status/{execution_id}")
async def get_batch_status(execution_id: str):
    execution = db.get_execution(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    return execution

@app.get("/batch/logs/{execution_id}")
async def get_batch_logs(execution_id: str):
    logs = db.get_logs(execution_id)
    return {"logs": logs}

@app.get("/batch/jobs")
async def get_jobs():
    jobs = db.get_all_jobs()
    return {"jobs": jobs}

@app.get("/batch/stats")
async def get_stats():
    stats = db.get_batch_stats()
    return {"stats": stats}

@app.put("/batch/jobs/{job_id}/cron")
async def update_job_cron(job_id: int, request: dict):
    try:
        cron_expression = request.get('cron_expression')
        if not cron_expression:
            raise HTTPException(status_code=400, detail="cron_expression is required")
        
        affected_rows = db.update_job_cron(job_id, cron_expression)
        if affected_rows == 0:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return {"message": "Cron expression updated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Update cron error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8092)