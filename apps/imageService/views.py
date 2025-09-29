from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import JSONParser
from django.conf import settings
import re
import logging
from .services import GeoServerService, NDVIProcessor, NDWIProcessor, KafkaService

logger = logging.getLogger(__name__)

class NDVIProcessingView(APIView):
    parser_classes = [JSONParser]
    
    def post(self, request):
        kafka_service = KafkaService()
        workspace = None
        layer = None
        ndvi_layer_name = None
        
        try:
            layer_name = request.data.get('layer_name')
            
            if not self._validate_layer_format(layer_name):
                return Response({
                    'error': 'Invalid layer format. Expected workspace:layer_name'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            workspace, layer = layer_name.split(':')
            ndvi_layer_name = f"{layer}_NDVI"
            
            geoserver_service = GeoServerService()
            file_path = geoserver_service.get_layer_file_path(workspace, layer)
            
            if not file_path:
                error_msg = f'Layer {layer_name} not found in GeoServer'
                kafka_service.publish_failure(
                    workspace=workspace,
                    store_name=ndvi_layer_name,
                    layer_type="NDVI",
                    original_layer=layer,
                    error_message=error_msg
                )
                return Response({
                    'error': error_msg
                }, status=status.HTTP_404_NOT_FOUND)
            
            if geoserver_service.check_layer_exists(workspace, ndvi_layer_name):
                error_msg = f'NDVI layer {workspace}:{ndvi_layer_name} already exists'
                kafka_service.publish_failure(
                    workspace=workspace,
                    store_name=ndvi_layer_name,
                    layer_type="NDVI",
                    original_layer=layer,
                    error_message=error_msg
                )
                return Response({
                    'error': error_msg
                }, status=status.HTTP_409_CONFLICT)
            
            ndvi_processor = NDVIProcessor()
            output_path = ndvi_processor.process_ndvi(file_path, workspace, layer)
            
            if not output_path:
                error_msg = 'NDVI processing failed'
                kafka_service.publish_failure(
                    workspace=workspace,
                    store_name=ndvi_layer_name,
                    layer_type="NDVI",
                    original_layer=layer,
                    error_message=error_msg
                )
                return Response({
                    'error': error_msg
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            success = geoserver_service.publish_ndvi_layer(
                workspace, ndvi_layer_name, output_path
            )
            
            if not success:
                error_msg = 'Failed to publish NDVI layer to GeoServer'
                self._cleanup_ndvi_file(output_path)
                kafka_service.publish_failure(
                    workspace=workspace,
                    store_name=ndvi_layer_name,
                    layer_type="NDVI",
                    original_layer=layer,
                    error_message=error_msg
                )
                return Response({
                    'error': error_msg
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            # Publish success message to Kafka
            kafka_service.publish_success(
                workspace=workspace,
                store_name=ndvi_layer_name,
                layer_type="NDVI",
                original_layer=layer,
                file_path=output_path
            )
            
            return Response({
                'message': f'NDVI layer {ndvi_layer_name} successfully created',
                'layer_name': f'{workspace}:{ndvi_layer_name}',
                'file_path': output_path
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            error_msg = 'Internal processing error'
            logger.error(f"NDVI processing error: {str(e)}")
            
            if workspace and ndvi_layer_name and layer:
                kafka_service.publish_failure(
                    workspace=workspace,
                    store_name=ndvi_layer_name,
                    layer_type="NDVI",
                    original_layer=layer,
                    error_message=f"Internal error: {str(e)}"
                )
            
            return Response({
                'error': error_msg
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            kafka_service.close()
    
    def _validate_layer_format(self, layer_name):
        if not layer_name:
            return False
        pattern = r'^[a-zA-Z0-9_-]+:[a-zA-Z0-9_-]+$'
        return bool(re.match(pattern, layer_name))
    
    def _cleanup_ndvi_file(self, file_path):
        try:
            import os
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Cleaned up NDVI file: {file_path}")
        except Exception as e:
            logger.warning(f"Failed to cleanup NDVI file: {str(e)}")


class NDWIProcessingView(APIView):
    parser_classes = [JSONParser]
    
    def post(self, request):
        kafka_service = KafkaService()
        workspace = None
        layer = None
        ndwi_layer_name = None
        
        try:
            layer_name = request.data.get('layer_name')
            
            if not self._validate_layer_format(layer_name):
                return Response({
                    'error': 'Invalid layer format. Expected workspace:layer_name'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            workspace, layer = layer_name.split(':')
            ndwi_layer_name = f"{layer}_NDWI"
            
            geoserver_service = GeoServerService()
            file_path = geoserver_service.get_layer_file_path(workspace, layer)
            
            if not file_path:
                error_msg = f'Layer {layer_name} not found in GeoServer'
                kafka_service.publish_failure(
                    workspace=workspace,
                    store_name=ndwi_layer_name,
                    layer_type="NDWI",
                    original_layer=layer,
                    error_message=error_msg
                )
                return Response({
                    'error': error_msg
                }, status=status.HTTP_404_NOT_FOUND)
            
            if geoserver_service.check_layer_exists(workspace, ndwi_layer_name):
                error_msg = f'NDWI layer {workspace}:{ndwi_layer_name} already exists'
                kafka_service.publish_failure(
                    workspace=workspace,
                    store_name=ndwi_layer_name,
                    layer_type="NDWI",
                    original_layer=layer,
                    error_message=error_msg
                )
                return Response({
                    'error': error_msg
                }, status=status.HTTP_409_CONFLICT)
            
            ndwi_processor = NDWIProcessor()
            output_path = ndwi_processor.process_ndwi(file_path, workspace, layer)
            
            if not output_path:
                error_msg = 'NDWI processing failed'
                kafka_service.publish_failure(
                    workspace=workspace,
                    store_name=ndwi_layer_name,
                    layer_type="NDWI",
                    original_layer=layer,
                    error_message=error_msg
                )
                return Response({
                    'error': error_msg
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            success = geoserver_service.publish_ndvi_layer(
                workspace, ndwi_layer_name, output_path
            )
            
            if not success:
                error_msg = 'Failed to publish NDWI layer to GeoServer'
                self._cleanup_ndwi_file(output_path)
                kafka_service.publish_failure(
                    workspace=workspace,
                    store_name=ndwi_layer_name,
                    layer_type="NDWI",
                    original_layer=layer,
                    error_message=error_msg
                )
                return Response({
                    'error': error_msg
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            # Publish success message to Kafka
            kafka_service.publish_success(
                workspace=workspace,
                store_name=ndwi_layer_name,
                layer_type="NDWI",
                original_layer=layer,
                file_path=output_path
            )
            
            return Response({
                'message': f'NDWI layer {ndwi_layer_name} successfully created',
                'layer_name': f'{workspace}:{ndwi_layer_name}',
                'file_path': output_path
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            error_msg = 'Internal processing error'
            logger.error(f"NDWI processing error: {str(e)}")
            
            if workspace and ndwi_layer_name and layer:
                kafka_service.publish_failure(
                    workspace=workspace,
                    store_name=ndwi_layer_name,
                    layer_type="NDWI",
                    original_layer=layer,
                    error_message=f"Internal error: {str(e)}"
                )
            
            return Response({
                'error': error_msg
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            kafka_service.close()
    
    def _validate_layer_format(self, layer_name):
        if not layer_name:
            return False
        pattern = r'^[a-zA-Z0-9_-]+:[a-zA-Z0-9_-]+$'
        return bool(re.match(pattern, layer_name))
    
    def _cleanup_ndwi_file(self, file_path):
        try:
            import os
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Cleaned up NDWI file: {file_path}")
        except Exception as e:
            logger.warning(f"Failed to cleanup NDWI file: {str(e)}")
