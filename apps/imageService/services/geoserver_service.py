import requests
import json
import logging
from urllib.parse import urlparse
from django.conf import settings
from urllib.parse import urlparse, unquote
from urllib.request import url2pathname

logger = logging.getLogger(__name__)

class GeoServerService:
    
    def __init__(self):
        self.base_url = getattr(settings, 'GEOSERVER_BASE_URL', 'http://localhost:8080/geoserver')
        self.rest_url = f"{self.base_url}/rest"
        self.username = getattr(settings, 'GEOSERVER_USERNAME', 'admin')
        self.password = getattr(settings, 'GEOSERVER_PASSWORD', 'geoserver')
        self.auth = (self.username, self.password)
    
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
    
    # def _extract_file_path(self, file_url):
    #     """Extract local file path from file:// URL"""
    #     parsed_url = urlparse(file_url)
    #     return parsed_url.path
    
    def _extract_file_path(self, file_url):
        """Extract local file path from file:// URL (cross-platform)"""
        parsed_url = urlparse(file_url)
        
        # Reconstruct path including netloc if it contains drive letter
        if parsed_url.netloc and ':' in parsed_url.netloc:
            # Windows case: file://D:/... -> netloc='D:', path='/data/...'
            full_path = '/' + parsed_url.netloc + parsed_url.path
        else:
            full_path = parsed_url.path
        
        #return url2pathname(unquote(full_path))
        standard_path = url2pathname(unquote(full_path))
        logger.info(f"Standard path: {standard_path}")
        
        # Convert Windows path to Docker mount path
        docker_path = self._convert_to_docker_path(standard_path)
        logger.info(f"Docker path: {docker_path}")
        
        return docker_path
    

    def _convert_to_docker_path(self, standard_path):
        """Convert path to Docker mount format"""
        # Simple fix: remove colon after drive letter
        # /D:/data/... -> /D/data/...
        if ':' in standard_path and standard_path.startswith('/'):
            docker_path = standard_path.replace(':/', '/')
            logger.info(f"Fixed colon in path: {standard_path} -> {docker_path}")
            return docker_path
        
        return standard_path
    
    def _convert_docker_path_to_windows_path(self, docker_path):
        """Convert Docker mount path back to Windows path for GeoServer"""
        # /D/data/... -> D:/data/...
        if docker_path.startswith('/') and len(docker_path) >= 3 and docker_path[2] == '/':
            drive_letter = docker_path[1]
            rest_path = docker_path[2:]
            windows_path = f"{drive_letter}:{rest_path}"
            logger.info(f"Converted Docker path to Windows: {docker_path} -> {windows_path}")
            return windows_path
        
        return docker_path
        
    def _validate_file_exists(self, file_path):
        """Validate that the file exists on local filesystem"""
        import os
        return os.path.exists(file_path) and os.path.isfile(file_path)
    
    def _create_coverage_store(self, workspace, store_name, file_path):
        """Create GeoServer coverage store for NDVI layer"""
        try:
            url = f"{self.rest_url}/workspaces/{workspace}/coveragestores"
            windows_file_path = self._convert_docker_path_to_windows_path(file_path)
            payload = {
                "coverageStore": {
                    "name": store_name,
                    "type": "GeoTIFF",
                    "enabled": True,
                    "workspace": {
                        "name": workspace
                    },
                    "url": f"file://{windows_file_path}"
                }
            }
            
            logger.info(f"Creating coverage store: {store_name} in workspace: {workspace}")
            logger.info(f"File path: {windows_file_path}")
            
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
                logger.error(f"URL: {url}")
                return False
            
            logger.info(f"Coverage store {store_name} created successfully")
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
    
    # def _apply_ndvi_style(self, workspace, layer_name):
    #     """Apply NDVI color ramp styling (placeholder for next step)"""
    #     logger.info(f"NDVI styling will be applied to {workspace}:{layer_name}")
    #     pass


    # def _apply_ndvi_style(self, workspace, layer_name):
    #     """Apply NDVI color ramp styling"""
    #     try:
    #         style_name = f"{layer_name}_style"
            
    #         if self._create_ndvi_style(workspace, style_name):
    #             self._assign_style_to_layer(workspace, layer_name, style_name)
    #             logger.info(f"NDVI styling applied to {workspace}:{layer_name}")
    #         else:
    #             logger.warning(f"Failed to apply styling to {workspace}:{layer_name}")
                
    #     except Exception as e:
    #         logger.error(f"Styling error: {str(e)}")

    # def _create_ndvi_style(self, workspace, style_name):
    #     """Create SLD style for NDVI visualization"""
    #     try:
    #         sld_content = self._generate_ndvi_sld()
            
    #         # Create style in workspace
    #         url = f"{self.rest_url}/workspaces/{workspace}/styles"
            
    #         style_payload = {
    #             "style": {
    #                 "name": style_name,
    #                 "filename": f"{style_name}.sld"
    #             }
    #         }
            
    #         response = requests.post(
    #             url,
    #             json=style_payload,
    #             headers={'Content-Type': 'application/json'},
    #             auth=self.auth,
    #             timeout=30
    #         )
            
    #         if response.status_code != 201:
    #             logger.error(f"Style creation failed: {response.status_code}")
    #             return False
            
    #         # Upload SLD content
    #         sld_url = f"{url}/{style_name}"
    #         sld_response = requests.put(
    #             sld_url,
    #             data=sld_content,
    #             headers={'Content-Type': 'application/vnd.ogc.sld+xml'},
    #             auth=self.auth,
    #             timeout=30
    #         )
            
    #         sld_response.raise_for_status()
    #         return True
            
    #     except requests.RequestException as e:
    #         logger.error(f"SLD creation failed: {str(e)}")
    #         return False

    # def _assign_style_to_layer(self, workspace, layer_name, style_name):
    #     """Assign style to layer as default"""
    #     try:
    #         url = f"{self.rest_url}/layers/{workspace}:{layer_name}"
            
    #         payload = {
    #             "layer": {
    #                 "defaultStyle": {
    #                     "name": f"{workspace}:{style_name}",
    #                     "workspace": workspace
    #                 }
    #             }
    #         }
            
    #         response = requests.put(
    #             url,
    #             json=payload,
    #             headers={'Content-Type': 'application/json'},
    #             auth=self.auth,
    #             timeout=30
    #         )
            
    #         response.raise_for_status()
    #         return True
            
    #     except requests.RequestException as e:
    #         logger.error(f"Style assignment failed: {str(e)}")
    #         return False

    # def _generate_ndvi_sld(self):
    #     """Generate SLD XML for NDVI color ramp"""
        
    #     # NDVI breakpoints and colors from your specification
    #     ndvi_breaks = [-0.5, -0.2, -0.1, 0.0, 0.025, 0.05, 0.075, 0.1, 0.125, 
    #                 0.15, 0.175, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 
    #                 0.55, 0.6, 1.0]
        
    #     colors_hex = [
    #         "#0c0c0c", "#bfbfbf", "#dbdbdb", "#eaeaea", "#fff9cc", "#ede8b5", 
    #         "#ddd89b", "#ccc682", "#bcb76b", "#afc160", "#a3cc59", "#91bf51",
    #         "#7fb247", "#70a33f", "#609635", "#4f892d", "#3f7c23", "#306d1c",
    #         "#216011", "#0f540a", "#004400"
    #     ]
        
    #     # Generate color map entries
    #     color_map_entries = []
    #     for i, (value, color) in enumerate(zip(ndvi_breaks, colors_hex)):
    #         color_map_entries.append(
    #             f'<ColorMapEntry color="{color}" quantity="{value}" opacity="1.0"/>'
    #         )
        
    #     sld_template = f'''<?xml version="1.0" encoding="UTF-8"?>
    # <StyledLayerDescriptor version="1.0.0" 
    #     xmlns="http://www.opengis.net/sld" 
    #     xmlns:ogc="http://www.opengis.net/ogc" 
    #     xmlns:xlink="http://www.w3.org/1999/xlink" 
    #     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    # <NamedLayer>
    #     <Name>NDVI</Name>
    #     <UserStyle>
    #     <Title>NDVI Color Ramp</Title>
    #     <Abstract>NDVI visualization with 21-point color classification</Abstract>
    #     <FeatureTypeStyle>
    #         <Rule>
    #         <RasterSymbolizer>
    #             <ColorMap type="ramp">
    #             {chr(10).join(color_map_entries)}
    #             </ColorMap>
    #         </RasterSymbolizer>
    #         </Rule>
    #     </FeatureTypeStyle>
    #     </UserStyle>
    # </NamedLayer>
    # </StyledLayerDescriptor>'''
        
    #     return sld_template
