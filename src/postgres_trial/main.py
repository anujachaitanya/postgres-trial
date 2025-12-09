
import fastapi
from pydantic import BaseModel
from src.postgres_trial.clients.sql_client import PostgresClient
from src.postgres_trial.db.database import execute_read_query
import os

from src.postgres_trial.models.user import User

client = PostgresClient()

session = client.init(host=os.getenv("DATABASE_HOST"),
            database=os.getenv("DATABASE_NAME"),
            user=os.getenv("DATABASE_USER"),
            password=os.getenv("DATABASE_PASSWORD"),
            port=int(os.getenv("DATABASE_PORT", 5432)))


app = fastapi.FastAPI()

@app.get("/")
async def read_root():
    return {"Hello": "World"}

@app.get("/users")
async def read_users():
    users = client.session.query(User).all()
    return users


@app.post("/users")
async def create_user(name: str, email: str):
    user = User(name=name, email=email)
    client.session.add(user)
    client.session.commit()
    return {"message": "User created successfully"}


class SQLQuery(BaseModel):
    query: str
    class Config:
        max_anystr_length = 500


@app.post("/query")
async def execute_query(sql_query: SQLQuery):
    """
    Executes a SELECT query submitted by the user.
    Uses a read-only user and enforces time/row limits.
    """
    try:
        results = execute_read_query(sql_query.query, client.session)
        return {
            "status": "success",
            "count": len(results),
            "data": results
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=f"Query Execution Failed: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")