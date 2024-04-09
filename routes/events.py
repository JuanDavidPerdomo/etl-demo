from fastapi import APIRouter

events_router = APIRouter()


@events_router.get("/")
async def root():
    return {"message": "The Server is Working!"}
