@echo off
REM Test Databricks LLM API response format

set DATABRICKS_URL=https://dbc-3247cc85-ef1e.cloud.databricks.com/serving-endpoints/databricks-meta-llama-3-3-70b-instruct/invocations
set DATABRICKS_TOKEN=%DATABRICKS_LLM_TOKEN%

if "%DATABRICKS_TOKEN%"=="" (
    echo ERROR: DATABRICKS_LLM_TOKEN not set
    exit /b 1
)

echo Testing Databricks LLM API...
echo ===============================
echo.

curl -X POST "%DATABRICKS_URL%" ^
  -H "Authorization: Bearer %DATABRICKS_TOKEN%" ^
  -H "Content-Type: application/json" ^
  -d "{\"messages\": [{\"role\": \"system\", \"content\": \"Convert Oracle DDL to Databricks SQL.\"}, {\"role\": \"user\", \"content\": \"Convert this Oracle DDL to Databricks SQL: CREATE TABLE TEST (ID NUMBER(10));\"}], \"temperature\": 0.1, \"max_tokens\": 8192}" ^
  -v 2>&1

echo.
echo ===============================
echo Test complete.
pause
