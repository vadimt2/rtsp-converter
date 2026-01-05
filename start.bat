@echo off
echo ========================================
echo Road-AI RTSP Stream Server
echo ========================================
echo.
echo Starting services...
docker-compose up --build -d
echo.
echo ========================================
echo Stream will be available at:
echo   RTSP: rtsp://localhost:8554/abbeyroad
echo   HLS:  http://localhost:8888/abbeyroad
echo   Web:  http://localhost:8889/abbeyroad
echo ========================================
echo.
echo View logs with: docker-compose logs -f
echo Stop with: docker-compose down
echo.
pause
