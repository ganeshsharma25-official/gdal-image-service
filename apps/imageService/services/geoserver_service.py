import os
import requests
import json
import logging
from django.conf import settings
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class GeoServerService:
    
    def __init__(self):
        self.base_url = getattr(settings, 'GEOSERVER_BASE_URL', 'http://localhost:8080/geoserver')
        self.rest_url = f"{self.base_url}/rest"
        self.username = getattr(settings, 'GEOSERVER_USERNAME', 'admin')
        self.password = getattr(settings, 'GEOSERVER_PASSWORD', 'geoserver')
        self.auth = (self.username, self.password)
        self.data_path = getattr(settings, 'DATA_PATH', '/data')
        self.container_data_path = getattr(settings, 'CONTAINER_DATA_PATH', '/data')
    
    def get_layer_file_path(self, workspace, layer_name):
        """
        Extract file path from GeoServer coverage store metadata
        Returns file path or None if layer not found
        """
        try:
            coverage_store_url = (
                f"{self.rest_url}/workspaces/{workspace}/"
                f"coveragestores/{layer_name}.json"
            )
            
            response = requests.get(coverage_store_url, auth=self.auth, timeout=30)
            
            if response.status_code == 404:
                logger.warning(f"Coverage store {workspace}:{layer_name} not found")
                return None
            
            response.raise_for_status()
            data = response.json()
            
            file_url = data['coverageStore']['url']
            file_path = self._extract_file_path(file_url)
            
            if not self._validate_file_exists(file_path):
                logger.error(f"File not found at path: {file_path}")
                return None
            
            return file_path
            
        except requests.RequestException as e:
            logger.error(f"GeoServer request failed: {str(e)}")
            return None
        except (KeyError, json.JSONDecodeError) as e:
            logger.error(f"Invalid GeoServer response format: {str(e)}")
            return None
    
    def check_layer_exists(self, workspace, layer_name):
        """
        Check if NDVI layer already exists to prevent overwrite
        """
        try:
            layer_url = f"{self.rest_url}/workspaces/{workspace}/layers/{layer_name}.json"
            response = requests.get(layer_url, auth=self.auth, timeout=30)
            return response.status_code == 200
        except requests.RequestException:
            return False
    
    def publish_ndvi_layer(self, workspace, layer_name, file_path):
        """Publish processed NDVI layer to GeoServer (no styling needed)"""
        try:
            if self.check_layer_exists(workspace, layer_name):
                logger.error(f"Layer {workspace}:{layer_name} already exists")
                return False
            
            coverage_store_created = self._create_coverage_store(
                workspace, layer_name, file_path
            )
            
            if not coverage_store_created:
                return False
            
            coverage_created = self._create_coverage(workspace, layer_name)
            return coverage_created
            
        except Exception as e:
            logger.error(f"Failed to publish NDVI layer: {str(e)}")
            return False
    
    def _extract_file_path(self, file_url):
        """Extract local file path from file:// URL"""
        parsed_url = urlparse(file_url)
        return parsed_url.path   
        
    def _validate_file_exists(self, file_path):
        """Validate that the file exists on local filesystem"""
        import os
        return os.path.exists(file_path) and os.path.isfile(file_path)
    

    def _create_coverage_store(self, workspace, store_name, file_path):
        """Create GeoServer coverage store for NDVI layer"""
        try:
            url = f"{self.rest_url}/workspaces/{workspace}/coveragestores"
            
            payload = {
                "coverageStore": {
                    "name": store_name,
                    "type": "GeoTIFF",
                    "enabled": True,
                    "workspace": {
                        "name": workspace
                    },
                    "url": f"file://{file_path}"
                }
            }
            
            logger.info(f"Creating coverage store: {store_name} in workspace: {workspace}")
            logger.info(f"File path: {file_path}")
            
            response = requests.post(
                url, 
                json=payload,
                headers={'Content-Type': 'application/json'},
                auth=self.auth,
                timeout=60
            )
            
            if response.status_code != 201:
                logger.error(f"Coverage store creation failed:")
                logger.error(f"Status: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Coverage store creation error: {str(e)}")
            return False
    
    def _create_coverage(self, workspace, layer_name):
        """Create GeoServer coverage (layer) from coverage store"""
        try:
            url = (f"{self.rest_url}/workspaces/{workspace}/"
                   f"coveragestores/{layer_name}/coverages")
            
            payload = {
                "coverage": {
                    "name": layer_name,
                    "title": f"{layer_name}",
                    "enabled": True
                }
            }
            
            response = requests.post(
                url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                auth=self.auth,
                timeout=60
            )
            
            response.raise_for_status()
            return True
            
        except requests.RequestException as e:
            logger.error(f"Coverage creation failed: {str(e)}")
            return False
