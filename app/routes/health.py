from fastapi import APIRouter
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter()


@router.get("/")
def health_check():
    return {"status": "API is up and running!"}