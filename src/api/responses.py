"""
Standard response utilities for the NFL Analytics Platform API

Provides consistent response formatting and error handling.
"""

from fastapi import HTTPException
from fastapi.responses import JSONResponse
from typing import Any, Dict, Optional
from datetime import datetime


def success_response(data: Any, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Create a standardized success response"""
    response = {
        "status": "success",
        "data": data,
        "metadata": {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "version": "7.0.0"
        }
    }
    
    if metadata:
        response["metadata"].update(metadata)
    
    return response


def error_response(
    code: str, 
    message: str, 
    details: Optional[Dict[str, Any]] = None,
    status_code: int = 400
) -> HTTPException:
    """Create a standardized error response"""
    error_detail = {
        "status": "error",
        "error": {
            "code": code,
            "message": message
        },
        "metadata": {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "version": "7.0.0"
        }
    }
    
    if details:
        error_detail["error"]["details"] = details
    
    return HTTPException(status_code=status_code, detail=error_detail)


def validation_error_response(
    message: str,
    validation_errors: list,
    status_code: int = 422
) -> HTTPException:
    """Create a validation error response"""
    error_detail = {
        "status": "error", 
        "error": {
            "code": "VALIDATION_ERROR",
            "message": message
        },
        "validation_errors": validation_errors,
        "metadata": {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "version": "7.0.0"
        }
    }
    
    return HTTPException(status_code=status_code, detail=error_detail)


def async_task_response(task_id: str, message: str = "Task started") -> Dict[str, Any]:
    """Create response for async task initiation"""
    return success_response(
        data={
            "task_id": task_id,
            "status": "pending",
            "message": message,
            "progress_percent": 0
        }
    )


# Standard error codes
class ErrorCodes:
    DATASET_NOT_FOUND = "DATASET_NOT_FOUND"
    INVALID_YEAR = "INVALID_YEAR"
    EXTRACTION_FAILED = "EXTRACTION_FAILED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    MODEL_NOT_FOUND = "MODEL_NOT_FOUND"
    TRAINING_FAILED = "TRAINING_FAILED"
    PREDICTION_FAILED = "PREDICTION_FAILED"
    REALTIME_UNAVAILABLE = "REALTIME_UNAVAILABLE"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    RATE_LIMITED = "RATE_LIMITED"
    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"


# Common error responses
def dataset_not_found_error(dataset: str) -> HTTPException:
    available_datasets = [
        "pbp", "weekly", "seasonal", "weekly_rosters", "seasonal_rosters",
        "schedules", "team_desc", "officials", "combine", "draft_picks",
        "qbr", "weekly_pfr", "seasonal_pfr", "injuries", "depth_charts",
        "snap_counts", "ftn_data", "ngs_data", "players"
    ]
    
    return error_response(
        code=ErrorCodes.DATASET_NOT_FOUND,
        message=f"Dataset '{dataset}' not found",
        details={"available_datasets": available_datasets},
        status_code=404
    )


def invalid_year_error(dataset: str, year: int, start_year: Optional[int] = None) -> HTTPException:
    message = f"Year {year} is not valid for dataset '{dataset}'"
    details = {"dataset": dataset, "requested_year": year}
    
    if start_year:
        message += f". Dataset starts from year {start_year}"
        details["start_year"] = start_year
    
    return error_response(
        code=ErrorCodes.INVALID_YEAR,
        message=message,
        details=details,
        status_code=400
    )


def extraction_failed_error(dataset: str, reason: str) -> HTTPException:
    return error_response(
        code=ErrorCodes.EXTRACTION_FAILED,
        message=f"Failed to extract dataset '{dataset}': {reason}",
        details={"dataset": dataset, "reason": reason},
        status_code=500
    )


def model_not_found_error(model_name: str) -> HTTPException:
    available_models = ["fantasy_prediction", "consistency_analysis", "breakout_prediction"]
    
    return error_response(
        code=ErrorCodes.MODEL_NOT_FOUND,
        message=f"Model '{model_name}' not found",
        details={"available_models": available_models},
        status_code=404
    )


def rate_limited_error(retry_after: int = 60) -> HTTPException:
    return error_response(
        code=ErrorCodes.RATE_LIMITED,
        message="Rate limit exceeded. Please try again later.",
        details={"retry_after_seconds": retry_after},
        status_code=429
    )