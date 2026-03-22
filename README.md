# NBA Props Bar Chart App

A simple NBA props tracker using `nba_api` on the backend and HTML/CSS/JavaScript on the frontend.

## Features
- Team picker with roster cards and player headshots
- Backup player search
- Props supported: PTS, REB, AST, 3PM, STL, BLK, PRA, PR, PA, RA
- Hit rate, average, and recent game log table
- Simple in-memory caching and light request spacing to reduce repeated `nba_api` calls
- Render-ready config included

## Run locally

```bash
pip install -r requirements.txt
uvicorn main:app --reload
```

Open:

```text
http://127.0.0.1:8000
```

## Deploy on Render

This repo already includes both:
- `render.yaml`
- `.python-version`

### Option 1: Blueprint deploy
1. Push this folder to GitHub.
2. In Render, choose **New +** → **Blueprint**.
3. Select your GitHub repo.
4. Render will detect `render.yaml` and create the web service.

### Option 2: Manual web service
Use these settings:
- **Runtime:** Python
- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `uvicorn main:app --host 0.0.0.0 --port $PORT`
- **Health Check Path:** `/health`

## Notes
- `nba_api` does not require an API key.
- NBA stats endpoints can throttle or time out sometimes, so this app adds caching and avoids direct browser calls.
- On Render free tier, the service may take a bit longer on the first request after sleeping.
- You can optionally pass a season manually like `2025-26`.
