
import fastapi

from src.postgres_trial.clients.sql_client import PostgresClient
import os

client = PostgresClient()

client.init(host=os.getenv("DATABASE_HOST"),
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
    users = client.fetch("SELECT * FROM users")
    return {"users": [dict(user._mapping) for user in users]}


@app.post("/users")
async def create_user(name: str, email: str):
    client.fetch(
        "INSERT INTO users (name, email) VALUES (:name, :email)",
        {"name": name, "email": email}
    )
    return {"message": "User created successfully"}