#!/usr/bin/env python3

import json
import asyncio
import sys

from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware

from utils.env import IO_TIMEOUT, CORS_ALLOW_ORIGINS, CORS_ALLOW_CREDENTIALS
from lib.yt_data import fetch_multiple_videos
from lib.scraper import scrape_multiple_videos
from pydantic import BaseModel
from typing import List, Dict, Optional
from datetime import datetime



app = FastAPI()


# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[CORS_ALLOW_ORIGINS],
    allow_credentials=CORS_ALLOW_CREDENTIALS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store scraping jobs and their progress
cached_jobs: Dict[str, dict] = {}


class ScrapeRequest(BaseModel):
    video_ids: List[str]


yt_data_tools = {
    "scrape": {
        "description": "Scrape youtube.com",
        "task": scrape_multiple_videos
    },
    "fetch": {
        "description": f"Fetch videos using the YouTube Data API v3",
        "task": fetch_multiple_videos,
    },
}


async def run_tool(tool_name, job_id: str, video_ids: List[str]):

    pipeline_kwargs = {}
    pipeline_kwargs["csv_output_path"] = f"data/{job_id}.csv"
    pipeline_kwargs["name"] = f"{yt_data_tools[tool_name]['description']} - {job_id}"
    pipeline_kwargs["dry_run"] = False

    start_task = yt_data_tools[tool_name]['task'] 

    try:

        cached_jobs[job_id]["status"] = "running"
        total = len(video_ids)

        async def progress_callback(completed: int, current_video: str):
            cached_jobs[job_id]["progress"] = {
                "completed": completed,
                "total": total,
                "current_video": current_video
            }

        results = await start_task(
            video_ids, progress_callback=progress_callback, **pipeline_kwargs)

        cached_jobs[job_id]["status"] = "completed"
        cached_jobs[job_id]["results"] = results

    except Exception as e:
        cached_jobs[job_id]["status"] = "failed"
        cached_jobs[job_id]["error"] = str(e)


@app.post("/{tool_name}")
async def start_tool(request: ScrapeRequest, background_tasks: BackgroundTasks, tool_name: str):

    if tool_name not in yt_data_tools:
        return {"error": f"Invalid request, no such tool: {tool_name}"}
    
    print(f"Starting scraping job with tool: {tool_name}")  
    print(f"Video IDs: {request.video_ids}")

    job_id = f"{tool_name}-{datetime.now().strftime("%Y%m%d_%H%M%S")}"
    cached_jobs[job_id] = {
        "status": "pending",
        "progress": {
            "completed": 0,
            "total": len(request.video_ids),
            "current_video": ""
        },
        "results": None,
        "error": None
    }

    background_tasks.add_task(run_tool, tool_name, job_id, request.video_ids)
    return {"job_id": job_id}


@app.get("/status/{job_id}")
async def get_status(job_id: str):

    if job_id not in cached_jobs:
        return {"error": "Job not found"}
    return cached_jobs[job_id]


@app.get("/results/{job_id}")
async def get_results(job_id: str):

    if job_id not in cached_jobs:
        return {"error": "Job not found"}

    if cached_jobs[job_id]["status"] != "completed":
        return {"error": "Job not completed"}

    return {"results": cached_jobs[job_id]["results"]}


@app.get("/")
async def get_version():
    return {
        "name": "ytdt-api",
        "description": "YouTube Data Tools API",    
        "version": "0.1.0"
    }


if __name__ == "__main__":

    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="debug")

