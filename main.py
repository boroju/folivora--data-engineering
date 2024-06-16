from fastapi import FastAPI
from app.api.routes.customer import customer
from app.api.routes import book as book_router
from docs import tags_metadata
import uvicorn

from dotenv import dotenv_values
from pymongo import MongoClient

config = dotenv_values(".env")

# https://www.mongodb.com/resources/languages/pymongo-tutorial

app = FastAPI(
    title="Data Engineering RestAPI for Folivora Vegan",
    description="Building by using FastAPI and MongoDB",
    version="0.0.1",
    openapi_tags=tags_metadata
)


@app.on_event("startup")
def startup_db_client():
    app.mongodb_client = MongoClient(config["ATLAS_URI"])
    app.database = app.mongodb_client[config["DB_NAME"]]


@app.on_event("shutdown")
def shutdown_db_client():
    app.mongodb_client.close()


@app.get("/")
def root():
    return {"Data Engineering RestAPI for Folivora Vegan!"}


app.include_router(customer)
app.include_router(book_router, tags=["books"], prefix="/book")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
