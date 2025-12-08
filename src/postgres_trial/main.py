
import fastapi

from src.postgres_trial.clients.sql_client import PostgresClient
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
    return {"message": "User created successfully"}