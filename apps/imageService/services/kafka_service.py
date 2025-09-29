import json
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from django.conf import settings

logger = logging.getLogger(__name__)

class KafkaService:
    
    def __init__(self):
        self.bootstrap_servers = getattr(settings, 'KAFKA_BOOTSTRAP_SERVERS', '10.208.26.232:9092')
        self.topic = getattr(settings, 'KAFKA_TOPIC', 'image-processing-status')
        self.producer = None
        self._initialize_producer()
    
    def _initialize_producer(self):
        """Initialize Kafka producer with error handling"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                retries=3,
                request_timeout_ms=30000,
                api_version=(0, 10, 1)
            )
            logger.info(f"Kafka producer initialized for {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {str(e)}")
            self.producer = None
    
    def publish_processing_status(self, workspace, store_name, layer_type, status, 
                                original_layer, file_path=None, error_message=None):
        """
        Publish image processing status to Kafka
        
        Args:
            workspace (str): GeoServer workspace name
            store_name (str): GeoServer store name
            layer_type (str): NDVI or NDWI
            status (str): success or failed
            original_layer (str): Original layer name
            file_path (str, optional): Path to processed file
            error_message (str, optional): Error message if failed
        """
        if not self.producer:
            logger.warning("Kafka producer not available, skipping message publication")
            return False
        
        try:
            message = {
                "workspace": workspace,
                "store_name": store_name,
                "layer_type": layer_type,
                "status": status,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "original_layer": original_layer
            }
            
            if status == "success" and file_path:
                message["file_path"] = file_path
            elif status == "failed" and error_message:
                message["error_message"] = error_message
            
            # Use workspace:store_name as message key for partitioning
            message_key = f"{workspace}:{store_name}"
            
            future = self.producer.send(
                self.topic,
                key=message_key,
                value=message
            )
            
            # Wait for message to be sent (with timeout)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Kafka message sent successfully:")
            logger.info(f"Topic: {record_metadata.topic}")
            logger.info(f"Partition: {record_metadata.partition}")
            logger.info(f"Offset: {record_metadata.offset}")
            logger.info(f"Message: {json.dumps(message, indent=2)}")
            
            return True
            
        except KafkaError as e:
            logger.error(f"Kafka publishing error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error publishing to Kafka: {str(e)}")
            return False
    
    def publish_success(self, workspace, store_name, layer_type, original_layer, file_path):
        """Publish success status"""
        return self.publish_processing_status(
            workspace=workspace,
            store_name=store_name,
            layer_type=layer_type,
            status="success",
            original_layer=original_layer,
            file_path=file_path
        )
    
    def publish_failure(self, workspace, store_name, layer_type, original_layer, error_message):
        """Publish failure status"""
        return self.publish_processing_status(
            workspace=workspace,
            store_name=store_name,
            layer_type=layer_type,
            status="failed",
            original_layer=original_layer,
            error_message=error_message
        )
    
    def close(self):
        """Close Kafka producer connection"""
        if self.producer:
            try:
                self.producer.close()
                logger.info("Kafka producer connection closed")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {str(e)}")
    
    def __del__(self):
        """Ensure producer is closed when object is destroyed"""
        self.close()
