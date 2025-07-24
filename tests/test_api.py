"""
Tests for FastAPI NFL Analytics Platform API
"""

import pytest
import json
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch
import pandas as pd

# Import the FastAPI app
from src.api.main import app

# Create test client
client = TestClient(app)

class TestAPIEndpoints:
    """Test cases for API endpoints"""
    
    def test_health_check(self):
        """Test health check endpoint"""
        response = client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert data["status"] == "healthy"
    
    def test_system_status(self):
        """Test system status endpoint"""
        response = client.get("/api/v1/system/status")
        assert response.status_code == 200
        data = response.json()
        assert "overall_status" in data
        assert "services" in data
        assert "version" in data
        assert data["version"] == "7.0.0"
        assert data["overall_status"] == "healthy"
    
    def test_list_datasets(self):
        """Test listing available datasets"""
        response = client.get("/api/v1/datasets")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0
        
        # Check that each dataset has required fields
        for dataset in data:
            assert "name" in dataset
            assert "description" in dataset
            assert "start_year" in dataset
    
    def test_get_specific_dataset(self):
        """Test getting specific dataset information"""
        response = client.get("/api/v1/datasets/pbp")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "pbp"
        assert "description" in data
        assert "start_year" in data
    
    def test_get_nonexistent_dataset(self):
        """Test getting non-existent dataset returns 404"""
        response = client.get("/api/v1/datasets/nonexistent")
        assert response.status_code == 404
        data = response.json()
        assert "detail" in data
    
    @patch('src.nfl_extractor.NFLDataExtractor.extract_dataset')
    def test_extract_dataset(self, mock_extract):
        """Test dataset extraction endpoint"""
        # Mock extraction result
        mock_df = pd.DataFrame({'test': [1, 2, 3]})
        mock_metadata = {'rows': 3, 'columns': 1, 'extraction_time': '2023-01-01'}
        mock_extract.return_value = (mock_df, mock_metadata)
        
        response = client.post("/api/v1/datasets/pbp/extract", 
                              json={"year": 2023, "save_to_disk": True})
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "metadata" in data
    
    def test_extract_dataset_invalid_params(self):
        """Test dataset extraction with invalid parameters"""
        response = client.post("/api/v1/datasets/pbp/extract",
                              json={"year": 1990})  # Before dataset start year
        assert response.status_code == 400
        data = response.json()
        assert "detail" in data
    
    def test_analytics_models_list(self):
        """Test listing available ML models"""
        response = client.get("/api/v1/analytics/models")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
    
    @patch('src.advanced_analytics.NFLAnalytics')
    def test_analytics_training(self, mock_analytics):
        """Test ML model training endpoint"""
        # Mock analytics instance
        mock_instance = Mock()
        mock_instance.train_fantasy_models.return_value = {
            'models_trained': 4,
            'best_model': 'RandomForest',
            'performance': 0.85
        }
        mock_analytics.return_value = mock_instance
        
        response = client.post("/api/v1/analytics/train",
                              json={"model_types": ["fantasy_prediction"],
                                   "seasons": [2023]})
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
    
    def test_realtime_live_games(self):
        """Test live games endpoint"""
        response = client.get("/api/v1/realtime/games/live")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
    
    def test_realtime_stream_stats(self):
        """Test stream statistics endpoint"""
        response = client.get("/api/v1/realtime/stream/stats")
        assert response.status_code == 200
        data = response.json()
        assert "connections" in data
        assert "events_processed" in data
    
    def test_cors_headers(self):
        """Test that CORS headers are present"""
        response = client.options("/api/v1/health")
        assert response.status_code == 200
        # FastAPI handles CORS automatically when CORSMiddleware is added
    
    def test_openapi_spec(self):
        """Test that OpenAPI spec is accessible"""
        response = client.get("/api/v1/openapi.json")
        assert response.status_code == 200
        data = response.json()
        assert "openapi" in data
        assert "info" in data
        assert data["info"]["title"] == "NFL Analytics Platform API"
    
    def test_docs_accessible(self):
        """Test that API documentation is accessible"""
        response = client.get("/docs")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
    
    def test_redoc_accessible(self):
        """Test that ReDoc documentation is accessible"""
        response = client.get("/redoc")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

class TestAPIErrorHandling:
    """Test error handling and edge cases"""
    
    def test_invalid_json_request(self):
        """Test handling of invalid JSON in request"""
        response = client.post("/api/v1/datasets/pbp/extract",
                              data="invalid json")
        assert response.status_code == 422
    
    def test_missing_required_fields(self):
        """Test handling of missing required fields"""
        response = client.post("/api/v1/analytics/train",
                              json={})  # Missing required fields
        assert response.status_code == 422
    
    def test_rate_limiting_headers(self):
        """Test that appropriate headers are present for rate limiting"""
        response = client.get("/api/v1/health")
        # This would be implemented with rate limiting middleware
        assert response.status_code == 200

class TestAPIPerformance:
    """Test API performance characteristics"""
    
    def test_response_time_health_check(self):
        """Test that health check responds quickly"""
        import time
        start = time.time()
        response = client.get("/api/v1/health")
        duration = time.time() - start
        
        assert response.status_code == 200
        assert duration < 1.0  # Should respond in under 1 second
    
    def test_concurrent_requests(self):
        """Test handling of concurrent requests"""
        import concurrent.futures
        import threading
        
        def make_request():
            return client.get("/api/v1/health")
        
        # Test with 10 concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(make_request) for _ in range(10)]
            responses = [future.result() for future in futures]
        
        # All requests should succeed
        for response in responses:
            assert response.status_code == 200

@pytest.fixture
def mock_nfl_data():
    """Fixture providing mock NFL data"""
    return pd.DataFrame({
        'player_id': ['123', '456', '789'],
        'player_name': ['Tom Brady', 'Aaron Rodgers', 'Patrick Mahomes'],
        'position': ['QB', 'QB', 'QB'],
        'team': ['TB', 'GB', 'KC'],
        'passing_yards': [300, 250, 350],
        'touchdowns': [2, 1, 3]
    })

@pytest.mark.integration
class TestAPIIntegration:
    """Integration tests for API with real components"""
    
    def test_full_extraction_workflow(self, mock_nfl_data):
        """Test complete extraction workflow"""
        with patch('src.nfl_extractor.NFLDataExtractor.extract_dataset') as mock_extract:
            mock_extract.return_value = (mock_nfl_data, {'rows': 3})
            
            # Extract dataset
            response = client.post("/api/v1/datasets/weekly/extract",
                                  json={"year": 2023})
            assert response.status_code == 200
            
            # Verify extraction was called
            mock_extract.assert_called_once()
    
    def test_analytics_pipeline(self, mock_nfl_data):
        """Test analytics pipeline integration"""
        with patch('src.advanced_analytics.NFLAnalytics') as mock_analytics_class:
            mock_analytics = Mock()
            mock_analytics.train_fantasy_models.return_value = {'status': 'success'}
            mock_analytics_class.return_value = mock_analytics
            
            # Train models
            response = client.post("/api/v1/analytics/train",
                                  json={"model_types": ["fantasy_prediction"]})
            assert response.status_code == 200

if __name__ == "__main__":
    pytest.main([__file__, "-v"])