@echo off
echo Starting Jadiss Batch API...

docker-compose up -d --build

echo Batch API is starting...
echo - Batch API: http://localhost:8092