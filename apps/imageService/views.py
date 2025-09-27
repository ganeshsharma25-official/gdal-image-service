from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
import re
import logging
from .services import GeoServerService, NDVIProcessor

logger = logging.getLogger(__name__)

class NDVIProcessingView(APIView):
    
    def post(self, request):
        try:
            layer_name = request.data.get('layer_name')
            
            if not self._validate_layer_format(layer_name):
                return Response({
                    'error': 'Invalid layer format. Expected workspace:layer_name'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            workspace, layer = layer_name.split(':')
            
            geoserver_service = GeoServerService()
            file_path = geoserver_service.get_layer_file_path(workspace, layer)
            
            if not file_path:
                return Response({
                    'error': f'Layer {layer_name} not found in GeoServer'
                }, status=status.HTTP_404_NOT_FOUND)
            
            ndvi_layer_name = f"{layer}_NDVI"
            
            if geoserver_service.check_layer_exists(workspace, ndvi_layer_name):
                return Response({
                    'error': f'NDVI layer {workspace}:{ndvi_layer_name} already exists'
                }, status=status.HTTP_409_CONFLICT)
            
            ndvi_processor = NDVIProcessor()
            output_path = ndvi_processor.process_ndvi(file_path, workspace, layer)
            
            if not output_path:
                return Response({
                    'error': 'NDVI processing failed'
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            success = geoserver_service.publish_ndvi_layer(
                workspace, ndvi_layer_name, output_path
            )
            
            if not success:
                self._cleanup_ndvi_file(output_path)
                return Response({
                    'error': 'Failed to publish NDVI layer to GeoServer'
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            return Response({
                'message': f'NDVI layer {ndvi_layer_name} successfully created',
                'layer_name': f'{workspace}:{ndvi_layer_name}',
                'file_path': output_path
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            logger.error(f"NDVI processing error: {str(e)}")
            return Response({
                'error': 'Internal processing error'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _validate_layer_format(self, layer_name):
        if not layer_name:
            return False
        pattern = r'^[a-zA-Z0-9_-]+:[a-zA-Z0-9_-]+$'
        return bool(re.match(pattern, layer_name))
    
    def _cleanup_ndvi_file(self, file_path):
        """Clean up NDVI file if GeoServer publishing fails"""
        try:
            import os
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Cleaned up NDVI file: {file_path}")
        except Exception as e:
            logger.warning(f"Failed to cleanup NDVI file: {str(e)}")
