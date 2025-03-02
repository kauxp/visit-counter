from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any
from ....services.visit_counter import VisitCounterService
from ....schemas.counter import VisitCount

router = APIRouter()

# Dependency to get VisitCounterService instance
def get_visit_counter_service():
    return VisitCounterService()

@router.post("/visit/{page_id}")
async def record_visit(
    page_id: str,
    counter_service: VisitCounterService = Depends(get_visit_counter_service)
):
    """Record a visit for a website"""
    try:
        await counter_service.start_background_tasks()
        await counter_service.increment_visit(page_id)
        return {"status": "success", "message": f"Visit recorded for page {page_id}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/visits/{page_id}", response_model=VisitCount)
async def get_visits(
    page_id: str,
    counter_service: VisitCounterService = Depends(get_visit_counter_service)
):
    """Get visit count for a website"""
    try:
        await counter_service.start_background_tasks()
        count, via = await counter_service.get_visit_count(page_id)
        return VisitCount(visits=count, served_via=via)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
@router.post("/flush")
async def flush_buffer(
    counter_service: VisitCounterService = Depends(get_visit_counter_service)
):
    """Manually flush the buffered visit counts to Redis"""
    try:
        await counter_service.start_background_tasks()
        await counter_service.flush_all()
        return {"status": "success", "message": "Successfully flushed visit counts to Redis"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
@router.get("/buffer")
async def get_buffer_status(
    counter_service: VisitCounterService = Depends(get_visit_counter_service)
):
    """Get status of the in-memory buffer"""
    try:
        await counter_service.start_background_tasks()
        buffer_status = counter_service.get_buffer_status()
        return buffer_status
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 