from fastapi import FastAPI
from api.routes.customer import customer
from docs import tags_metadata
import uvicorn

app = FastAPI(
    title="Data Engineering RestAPI for Folivora Vegan",
    description="Building by using FastAPI and MongoDB",
    version="0.0.1",
    openapi_tags=tags_metadata
)

@app.get("/")
def root():
    return {"Data Engineering RestAPI for Folivora Vegan!"}


app.include_router(customer)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
