import os
import psutil
import requests
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from threading import Thread
import time
import shutil

app = FastAPI()

CAPACITY_MB = int(os.getenv("STORAGE_MB", "300"))
PEERS = [p.strip() for p in os.getenv("PEER_URLS", "").split(",") if p.strip()]

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

def get_usage_mb():
    total = 0
    for root, _, files in os.walk(DATA_DIR):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total / (1024 * 1024)

def get_total_capacity():
    total = CAPACITY_MB
    for peer in PEERS:
        try:
            r = requests.get(f"{peer}/capacity", timeout=2)
            total += r.json().get("capacity", 0)
        except:
            pass
    return total

def get_total_usage():
    total = get_usage_mb()
    for peer in PEERS:
        try:
            r = requests.get(f"{peer}/usage", timeout=2)
            total += r.json().get("usage", 0)
        except:
            pass
    return total

def replicate_to_peers(filepath, filename):
    for peer in PEERS:
        try:
            r = requests.get(f"{peer}/list", timeout=3)
            peer_files = [f["filename"] for f in r.json().get("files", [])]
            if filename in peer_files:
                continue
            with open(filepath, "rb") as f:
                requests.post(f"{peer}/replicate", files={"file": (filename, f)}, timeout=5)
        except Exception as e:
            print(f"[SYNC ERROR] {e}")

def sync_from_peers():
    for peer in PEERS:
        try:
            r = requests.get(f"{peer}/list", timeout=3)
            peer_files = r.json().get("files", [])
            for f in peer_files:
                filename = f["filename"]
                local_path = os.path.join(DATA_DIR, filename)
                if not os.path.exists(local_path):
                    fr = requests.get(f"{peer}/download/{filename}", timeout=5)
                    if fr.status_code == 200:
                        with open(local_path, "wb") as out:
                            out.write(fr.content)
        except Exception as e:
            print(f"[SYNC ERROR] {e}")

@app.get("/", response_class=HTMLResponse)
def index():
    with open("static_index.html") as f:
        return f.read()

@app.get("/capacity")
def capacity():
    return {"capacity": CAPACITY_MB}

@app.get("/usage")
def usage():
    return {"usage": get_usage_mb()}

@app.get("/total")
def total():
    return {
        "total_capacity": get_total_capacity(),
        "total_usage": get_total_usage()
    }

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    path = os.path.join(DATA_DIR, file.filename)
    if os.path.exists(path):
        return JSONResponse({"error": "File exists, overwrite not allowed"}, status_code=400)
    with open(path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    Thread(target=replicate_to_peers, args=(path, file.filename)).start()
    return {"status": "uploaded", "filename": file.filename, "size_mb": os.path.getsize(path)/(1024*1024)}

@app.post("/replicate")
async def replicate(file: UploadFile = File(...)):
    path = os.path.join(DATA_DIR, file.filename)
    if os.path.exists(path):
        return JSONResponse({"error": "Exists, skipped"}, status_code=409)
    with open(path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    return {"status": "replicated", "filename": file.filename}

@app.get("/download/{filename}")
def download(filename: str):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return JSONResponse({"error": "Not found"}, status_code=404)
    return FileResponse(path)

@app.get("/list")
def list_files():
    files = []
    for f in os.listdir(DATA_DIR):
        path = os.path.join(DATA_DIR, f)
        files.append({"filename": f, "size_mb": os.path.getsize(path)/(1024*1024)})
    return {"files": files}

@app.get("/health")
def health():
    return {"status": "ok", "service": "Storage server"}

@app.post("/ml/save")
async def ml_save(name: str = Form(...), file: UploadFile = File(...)):
    path = os.path.join(DATA_DIR, name)
    if os.path.exists(path):
        return JSONResponse({"error": "Exists, not allowed"}, status_code=400)
    with open(path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    Thread(target=replicate_to_peers, args=(path, name)).start()
    return {"status": "ml_saved", "name": name, "size_mb": os.path.getsize(path)/(1024*1024)}

@app.get("/ml/load/{name}")
def ml_load(name: str):
    path = os.path.join(DATA_DIR, name)
    if not os.path.exists(path):
        return JSONResponse({"error": "Not found"}, status_code=404)
    return FileResponse(path)

def keep_alive():
    while True:
        time.sleep(18)
        print("Heartbeat: alive")

def periodic_sync():
    while True:
        time.sleep(30)
        sync_from_peers()

Thread(target=keep_alive, daemon=True).start()
Thread(target=periodic_sync, daemon=True).start()
sync_from_peers()
