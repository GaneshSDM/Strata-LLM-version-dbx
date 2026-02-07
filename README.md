# Strata – Enterprise AI Translation Platform

An AI-powered multi-database migration platform that streamlines the process of migrating databases across different vendors using intelligent schema translation and comprehensive validation.

## Overview

Strata provides a complete 4-step workflow for database migration:

1. **Analyze** – Comprehensive source database analysis covering schema, tables, views, procedures, indexes, relationships, data types, security, and data profiling
2. **Extract** – Generate DDL scripts, dependency graphs, and conversion rules with full object inventory
3. **Migrate** – Two-phase migration (Structure then Data) using AI-powered DDL translation and chunked data transfer
4. **Reconcile** – Comprehensive validation across 5 categories with detailed reporting

## Supported Databases

- PostgreSQL
- MySQL
- Snowflake
- Databricks
- Oracle
- SQL Server
- Teradata
- Google BigQuery

## Features

- **AI-Powered Translation**: Uses OpenAI (via Replit AI Integrations) to intelligently translate schemas between database dialects
- **Secure Credential Storage**: Fernet encryption for database credentials
- **Dynamic Connection Forms**: Database-specific credential collection for all 8 supported databases
- **Session Management**: Locks source/target selection across the workflow for consistency
- **Progress Tracking**: Real-time progress updates during analysis, extraction, and migration
- **Comprehensive Validation**: Detailed validation reports with Pass/Fail status, error details, suggested fixes, and confidence scores
- **Multiple Export Formats**: Export validation reports as PDF, JSON, or Excel

## Tech Stack

### Backend
- **Python FastAPI** – REST API server
- **SQLite** – Metadata and connection storage
- **Cryptography (Fernet)** – Credential encryption
- **OpenAI API** – AI-powered DDL translation (via Replit AI Integrations)
- **Database Drivers**: psycopg2-binary, mysql-connector-python, snowflake-connector-python, databricks-sql-connector, oracledb, google-cloud-bigquery
- **Export Libraries**: reportlab (PDF), xlsxwriter (Excel)

### Frontend
- **React + Vite + TypeScript** – Modern web framework
- **TailwindCSS** – Utility-first styling
- **shadcn/ui** – High-quality UI components
- **React Router** – Client-side routing
- **Framer Motion** – Smooth animations
- **Lucide React** – Icon library

## Getting Started

### Prerequisites

The application is pre-configured to run on Replit with:
- Python 3.11
- Node.js 20
- OpenAI integration (uses Replit AI Integrations - no API key needed, billed to your credits)

### Installation

Dependencies are automatically installed. The project includes:

1. **Backend** (Python packages)
   ```bash
   pip install -r requirements.txt
   ```

2. **Frontend** (npm packages)
   ```bash
   npm install
   ```

### Running the Application

The application uses two workflows:

1. **Backend** – Runs on port 8000
   ```bash
   cd backend && uvicorn main:app --host 0.0.0.0 --port 8000
   ```
   Access it at `http://localhost:8000` in a browser.

2. **Frontend** – Runs on port 8001
   ```bash
   npm run dev
   ```

Both workflows start automatically when you run the project.

#### If your backend runs on a different port

The frontend proxies all `/api/*` calls to the backend. If you run the backend on a different port (e.g. `8080`), set one of these before starting `npm run dev`:

- `VITE_BACKEND_URL` (e.g. `http://localhost:8080`)
- `VITE_BACKEND_PORT` (e.g. `8080`)

## Usage Guide

### 1. Add Database Connections

1. Click the **Settings** (gear) icon in the top right
2. Fill in the connection details:
   - Connection Name
   - Database Type (select from dropdown)
   - Credentials (form changes based on database type)
3. Click **Test Connection** to verify
4. Click **Save Connection** once test succeeds

### 2. Analyze Source and Target

1. Navigate to the **Analyze** tab
2. Select your **Source Database** from saved connections
3. Select your **Target Database** from saved connections
4. Click **Start Analysis**
5. Wait for analysis to complete (shows progress through 10 phases)

### 3. Extract Data Objects

1. Navigate to the **Extract** tab
2. Click **Run Extraction**
3. View object counts when complete

### 4. Migrate Database

1. Navigate to the **Migrate** tab
2. Click **Migrate Structure** (creates tables, views, indexes, etc.)
3. Once structure migration completes, click **Migrate Data**
4. Wait for data transfer to complete

### 5. Validate and Export

1. Navigate to the **Reconcile** tab
2. Click **Run Validation**
3. Review validation results in the table
4. Export reports using:
   - **Export JSON** – Machine-readable format
   - **Export Excel** – Spreadsheet format
   - **Export PDF** – Formatted report

## API Endpoints

The backend provides a REST API at `/api`:

### Connections
- `POST /api/connections/test` – Test database connection
- `POST /api/connections/save` – Save encrypted connection
- `GET /api/connections` – List all connections

### Session Management
- `POST /api/session/set-source-target` – Lock source/target for run
- `GET /api/session` – Get current session details
- `POST /api/reset` – Clear session and artifacts

### Analysis
- `POST /api/analyze/start` – Start background analysis
- `GET /api/analyze/status` – Poll analysis progress

### Extraction
- `POST /api/extract/start` – Start extraction
- `GET /api/extract/status` – Get extraction status

### Migration
- `POST /api/migrate/structure` – Migrate database structure
- `POST /api/migrate/data` – Migrate table data

### Validation
- `POST /api/validate/run` – Run validation checks
- `GET /api/validate/report` – Get validation report

### Export
- `GET /api/export/json` – Download JSON report
- `GET /api/export/xlsx` – Download Excel report
- `GET /api/export/pdf` – Download PDF report

## Architecture

### Database Models (SQLite)

- **connections** – Stores encrypted database credentials
- **runs** – Tracks migration runs
- **artifacts** – Stores analysis/extraction artifacts
- **validation_reports** – Stores validation results
- **active_session** – Tracks current source/target selection

### Security

- All database credentials are encrypted using Fernet symmetric encryption
- Encryption key is auto-generated and stored locally if not provided via environment variable
- Credentials are never exposed in API responses

### AI Integration

The platform uses OpenAI for intelligent schema translation:
- Model: gpt-5 (via Replit AI Integrations)
- Use case: Translating DDL between database dialects
- Fallback: Deterministic Python-based translation rules
- Privacy: Only sends schema/DDL metadata, never credentials or data rows

## Driver Availability

Some database drivers may not be available in the Replit environment. When a driver is unavailable:
- The system gracefully falls back to "simulation mode"
- Test connections succeed but are marked as simulated
- Analysis, extraction, and migration return mock data
- This allows testing the full workflow even without real database access

## Development

### Project Structure

```
.
├── backend/
│   ├── adapters/          # Database-specific adapters
│   │   ├── postgresql.py
│   │   ├── mysql.py
│   │   ├── snowflake.py
│   │   ├── databricks.py
│   │   ├── oracle.py
│   │   ├── sqlserver.py
│   │   ├── teradata.py
│   │   └── bigquery.py
│   ├── models.py          # SQLite database models
│   ├── encryption.py      # Credential encryption
│   ├── ai.py             # OpenAI integration
│   └── main.py           # FastAPI application
├── src/
│   ├── components/       # React components
│   │   ├── Layout.tsx
│   │   └── ConnectionModal.tsx
│   ├── pages/           # Page components
│   │   ├── Analyze.tsx
│   │   ├── Extract.tsx
│   │   ├── Migrate.tsx
│   │   └── Reconcile.tsx
│   ├── App.tsx
│   └── main.tsx
├── artifacts/           # Runtime artifacts
├── package.json
└── requirements.txt
```

## Notes

- **OpenAI Integration**: This project uses Replit AI Integrations for OpenAI access, which does not require your own API key. Charges are billed to your Replit credits.
- **Port Configuration**: Frontend runs on port 8001, backend on port 8000
- **Accessing the backend**: `0.0.0.0` is only a bind address. In a browser, use `http://localhost:8000` (or your machine IP if accessing from another device).
- **Session Persistence**: Active sessions are stored in SQLite and persist across restarts
- **Artifact Storage**: Analysis and extraction results are saved to the `artifacts/` directory

## License

This is a demonstration application for educational purposes.
