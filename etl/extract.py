import requests
import json
from datetime import datetime
import os

def extract_jobs(BASE_URL):
    response = requests.get(BASE_URL, timeout=20)
    response.raise_for_status()
    payload = response.json()

    jobs = payload.get("data", [])
    print(f"Total jobs fetched: {len(jobs)}")

    os.makedirs("data", exist_ok=True)

    # timestamped file name
    # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # file_path = f"data/jobs_{timestamp}.json"
    
    file_path = f"data/bronze_jobs.json"
    

    # save file
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)

    print(f"Data saved to {file_path}")

    return jobs
