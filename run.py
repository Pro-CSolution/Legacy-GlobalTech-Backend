import uvicorn

if __name__ == "__main__":
    uvicorn.run("app.main:app_entry", host="0.0.0.0", port=8000, reload=True)

