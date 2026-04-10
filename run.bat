@echo off
echo.
echo  P^&G Scraper Tool
echo  ========================
echo.

python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python no encontrado. Instalalo desde https://python.org
    pause
    exit /b 1
)

echo [1/2] Ejecutando scraper...
echo.
cd /d "%~dp0"
python scraper.py

echo.
echo [2/2] Iniciando servidor local en http://localhost:8080
echo        Presiona Ctrl+C para detener el servidor
echo.
start "" http://localhost:8080
python -m http.server 8080
