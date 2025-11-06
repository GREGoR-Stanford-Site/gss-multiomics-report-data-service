from fastapi import FastAPI, File, UploadFile
from fastapi.concurrency import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from sqlalchemy import text
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

from app.db_manager import GSS_Database, close_db, initialize_db


# Global handles managed by lifespan events
db_handler = None


# Dependency to get the database instance
def get_db() -> GSS_Database:
    return db_handler


# Request models
class SearchRequestParams(BaseModel):
    filters: List[Dict[str, Any]] = Field(
        [],
        description='A list of dictionaries: each dict must contain the "chrom" key and optionally other field names and values.',
    )
    selected_fields: Optional[List[str]] = Field(
        [], description="A list of fields of records to return"
    )
    size: Optional[int] = Field(1, description="The max number of results to return.")
    offset: Optional[int] = Field(
        0, description="The number of rows in the matching result to skip."
    )
    return_lol: Optional[int] = Field(
        0,
        description="If set, the matched results will be returned as a list of tuples/lists.",
    )
    debug: Optional[int] = Field(0, description="Debug level")


class LoadRequestParams(BaseModel):
    file_name: Optional[str] = Field(
        "",
        description="Specify the name of the data file that is locally in the DATA_DIR, without uploading the file",
    )
    chunk_size: Optional[int] = Field(
        100, description="The number of rows to insert to the tables in bulk insert"
    )
    start_chunk: Optional[int] = Field(
        None, description="The starting number of chunk of rows to load."
    )
    chunk_count: Optional[int] = Field(
        None, description="The max number of chunks of rows to load."
    )
    valid_table_name_pat: Optional[str] = Field(
        "", description="The regular exp pattern for the table names to load"
    )
    skip_table_name_pat: Optional[str] = Field(
        "", description="The regular exp pattern for the table names to skip"
    )
    on_conflict: Optional[str] = Field(
        None,
        description="Specify the action in case there are records with conflicting keys: Possible actions: ignore (default), update ",
    )
    debug: Optional[int] = Field(0, description="Debug level")


class StatusResponse(BaseModel):
    success: bool = True
    message: Optional[str] = None
    data: Optional[Any] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_handler

    # Initialize PostgreSQL database
    db_handler = initialize_db()

    yield

    # Shutdown logic
    close_db()


# FastAPI app
app = FastAPI(lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=10000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# API endpoints
@app.post("/create_tables", response_model=StatusResponse, include_in_schema=False)
def create_tables(table_names: list):
    try:
        return db_handler.create_gsf_models(table_names)
    except Exception as e:
        return {"error": str(e)}


@app.post("/search")
async def search_api(params: SearchRequestParams):
    params_dict = params.model_dump()
    try:
        with db_handler.get_session() as session:
            return db_handler.search(session, **params_dict)
    except Exception as e:
        return {"Search error": str(e)}


@app.post("/query", include_in_schema=False)
async def execute_query(query: str):
    try:
        with db_handler.get_session() as session:
            result = session.execute(text(query))
            return [dict(row) for row in result]
    except Exception as e:
        return {"Query error": str(e)}


@app.post("/load_data", include_in_schema=False)
def load_data(params: LoadRequestParams, uploaded_file: UploadFile = File(None)):
    params_dict = params.model_dump() if params else {}
    try:
        with db_handler.get_session() as session:
            if uploaded_file:
                return db_handler.load(uploaded_file, session, **params_dict)
            else:
                file_name = params_dict.pop("file_name", None)
                if file_name:
                    return db_handler.load(file_name, session, **params_dict)
                else:
                    return {"Error": "No data file is given."}
    except Exception as e:
        return {"Loading error": str(e)}


@app.post("/drop_tables", include_in_schema=False)
def drop(table_name_pat: str = None):
    with db_handler.get_session() as session:
        return db_handler.drop_tables(table_name_pat, session)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
