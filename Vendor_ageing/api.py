# vendor_ageing/api.py

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional

from app_entry import run_query

# -------------------------------------------------
# FastAPI app
# -------------------------------------------------
app = FastAPI(
    title="Wave Finance Ageing Report API",
    version="1.0.0",
    description="Semantic NL → SQL API for customer ageing analysis"
)

# -------------------------------------------------
# Request schema
# -------------------------------------------------
class QuestionRequest(BaseModel):
    question: str
    execute: Optional[bool] = True

# -------------------------------------------------
# Endpoint
# -------------------------------------------------
@app.post("/run")
def run(req: QuestionRequest):
    try:
        result = run_query(req.question, execute=req.execute)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))