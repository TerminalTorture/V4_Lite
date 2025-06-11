import json
import logging
import os
from datetime import datetime
from threading import Lock
import paho.mqtt.client as mqtt
from typing import Dict, Any, Optional
import time

class MQTTUploader:
    """
    MQTT Client for uploading sensor data to an MQTT broker.
    Supports both individual sensor value publishing and batch data publishing.
    """
    
    def __init__(self, 
                 broker_host: str = None,
                 broker_port: int = 1883,
                 username: str = None,
                 password: str = None,
                 client_id: str = None,
                 keepalive: int = 60,
                 base_topic: str = "vflow"):
        """
        Initialize MQTT client with configuration.
        
        Args:
            broker_host: MQTT broker hostname/IP
            broker_port: MQTT broker port (default: 1883)
            username: MQTT username (optional)
            password: MQTT password (optional)
            client_id: MQTT client ID (optional, auto-generated if None)
            keepalive: Connection keepalive interval in seconds
            base_topic: Base topic prefix for all messages
        """
        
        # Load configuration from environment variables if not provided
        self.broker_host = broker_host or os.getenv('MQTT_BROKER_HOST', '192.168.137.69') #localhost
        self.broker_port = broker_port or int(os.getenv('MQTT_BROKER_PORT', '1883'))
        self.username = username or os.getenv('MQTT_USERNAME')
        self.password = password or os.getenv('MQTT_PASSWORD')
        self.client_id = client_id or os.getenv('MQTT_CLIENT_ID', f"vflow_client_{int(time.time())}")
        self.keepalive = keepalive
        self.base_topic = base_topic or os.getenv('MQTT_BASE_TOPIC', 'vflow')
        self.bulk_topic = os.getenv('MQTT_BULK_TOPIC', f"{self.base_topic}/data/bulk")
        
        # MQTT client setup
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish
        
        # Authentication if provided
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)
        
        # Connection state
        self.is_connected = False
        self._connection_lock = Lock()
        
        # QoS settings
        self.qos_level = int(os.getenv('MQTT_QOS_LEVEL', '1'))  # 0: at most once, 1: at least once, 2: exactly once
        
        # Enable/disable MQTT publishing
        self.enabled = os.getenv('MQTT_ENABLED', 'true').lower() in ('true', '1', 'yes', 'on')
        
        logging.info(f"MQTT Uploader initialized - Broker: {self.broker_host}:{self.broker_port}, Base Topic: {self.base_topic}, Enabled: {self.enabled}")
    
    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        """Callback for when the client receives a CONNACK response from the server."""
        if reason_code == 0:
            self.is_connected = True
            logging.info(f"✅ Connected to MQTT broker {self.broker_host}:{self.broker_port}")
        else:
            self.is_connected = False
            logging.error(f"❌ Failed to connect to MQTT broker. Reason code: {reason_code}")
    
    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        """Callback for when the client disconnects from the server."""
        self.is_connected = False
        if reason_code != 0:
            logging.warning(f"⚠️ Unexpected MQTT disconnection. Reason code: {reason_code}")
        else:
            logging.info("📡 Disconnected from MQTT broker")
    
    def _on_publish(self, client, userdata, mid, reason_code=None, properties=None):
        """Callback for when a message is published."""
        if reason_code and reason_code != 0:
            logging.error(f"❌ Failed to publish message {mid}. Reason code: {reason_code}")
        else:
            logging.debug(f"📤 Message {mid} published successfully")
    
    def connect(self) -> bool:
        """
        Connect to the MQTT broker.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        if not self.enabled:
            logging.info("MQTT publishing is disabled")
            return False
            
        with self._connection_lock:
            if self.is_connected:
                return True
            
            try:
                logging.info(f"🔗 Connecting to MQTT broker {self.broker_host}:{self.broker_port}...")
                self.client.connect(self.broker_host, self.broker_port, self.keepalive)
                self.client.loop_start()  # Start background network loop
                  # Wait for connection with timeout
                timeout = 10  # seconds
                start_time = time.time()
                while not self.is_connected and (time.time() - start_time) < timeout:
                    time.sleep(0.1)
                
                if self.is_connected:
                    return True
                else:
                    logging.error("❌ Connection timeout")
                    return False
                    
            except Exception as e:
                logging.error(f"❌ Error connecting to MQTT broker: {e}")
                return False
    
    def disconnect(self):
        """Disconnect from the MQTT broker."""
        if self.is_connected:
            self.client.loop_stop()
            self.client.disconnect()
            logging.info("📡 Disconnected from MQTT broker")
    
    def publish_sensor_data(self, sensor_data: Dict[str, Any]) -> bool:
        """
        Publish sensor data to MQTT broker.
        
        Args:
            sensor_data: Dictionary containing sensor readings
            
        Returns:
            bool: True if all messages published successfully, False otherwise
        """
        
        if not self.enabled:
            logging.info("📵 MQTT publishing is disabled - returning True")
            return True  # Return True to avoid blocking data flow when MQTT is disabled
            
        if not self.connect():
            logging.error("❌ Cannot publish: Not connected to MQTT broker")
            return False
        
        logging.info(f"✅ MQTT connected, proceeding with publish to {self.broker_host}:{self.broker_port}")
        
        try:
            # Prepare timestamp
            timestamp = sensor_data.get('timestamp')
            if isinstance(timestamp, str):
                # If timestamp is already ISO string, use it
                timestamp_str = timestamp
            else:
                # If timestamp is datetime object or None, convert to ISO string
                timestamp_str = datetime.now().isoformat() if timestamp is None else timestamp.isoformat()
            
            # Publish bulk data message
            # The sensor_data comes directly from live_data.py (flat structure with original casing)
            # We need to create the same structure as the /live-data endpoint
            bulk_payload = {k: v for k, v in sensor_data.items() if k != 'timestamp' and v is not None}
            
            # Add timestamp if it exists
            if 'timestamp' in sensor_data:
                bulk_payload['timestamp'] = timestamp_str
              # Ensure device_id is included if it exists in sensor_data
            if 'device_id' in sensor_data:
                bulk_payload['device_id'] = sensor_data['device_id']
            
            # bulk_topic = f\"{self.base_topic}/data/bulk\" # Old line
            payload_json = json.dumps(bulk_payload)
            logging.info(f"📤 Publishing to MQTT topic: {self.bulk_topic}")
            logging.info(f"📤 Payload JSON length: {len(payload_json)} characters")
            logging.debug(f"📤 Full payload: {payload_json}")
            
            result = self.client.publish(self.bulk_topic, payload_json, qos=self.qos_level)
            
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                logging.error(f"❌ Failed to publish bulk data. Error code: {result.rc}")
                return False
            
            
            # Optionally publish individual sensor values
            publish_individual = os.getenv('MQTT_PUBLISH_INDIVIDUAL', 'false').lower() in ('true', '1', 'yes', 'on')
            
            if publish_individual:
                success_count = 0
                total_count = 0
                
                for sensor_name, value in sensor_data.items():
                    if sensor_name == 'timestamp' or value is None:
                        continue
                    
                    total_count += 1
                    individual_topic = f"{self.base_topic}/sensors/{sensor_name}"
                    
                    individual_payload = {
                        'timestamp': timestamp_str,
                        'value': value,
                        'sensor': sensor_name,
                        'device_id': self.client_id
                    }
                    
                    result = self.client.publish(individual_topic, json.dumps(individual_payload), qos=self.qos_level)
                    
                    if result.rc == mqtt.MQTT_ERR_SUCCESS:
                        success_count += 1
                    else:
                        logging.error(f"❌ Failed to publish {sensor_name}. Error code: {result.rc}")
                
                return success_count == total_count
            
            return True
            
        except Exception as e:
            logging.error(f"❌ Error publishing sensor data to MQTT: {e}")
            return False
    
    def publish_status_message(self, status: str, message: str = None) -> bool:
        """
        Publish a status message to MQTT broker.
        
        Args:
            status: Status level (e.g., 'online', 'offline', 'error', 'warning')
            message: Optional status message
            
        Returns:
            bool: True if published successfully, False otherwise
        """
        if not self.enabled:
            return True
            
        if not self.connect():
            return False
        
        try:
            status_topic = f"{self.base_topic}/status"
            payload = {
                'timestamp': datetime.now().isoformat(),
                'device_id': self.client_id,
                'status': status,
                'message': message
            }
            
            result = self.client.publish(status_topic, json.dumps(payload), qos=self.qos_level)
            
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                return True
            else:
                logging.error(f"❌ Failed to publish status message. Error code: {result.rc}")
                return False
                
        except Exception as e:
            logging.error(f"❌ Error publishing status message to MQTT: {e}")
            return False


# Global MQTT uploader instance
_mqtt_uploader: Optional[MQTTUploader] = None

def get_mqtt_uploader() -> MQTTUploader:
    """Get or create the global MQTT uploader instance."""
    global _mqtt_uploader
    if _mqtt_uploader is None:
        _mqtt_uploader = MQTTUploader()
    return _mqtt_uploader

def upload_to_mqtt(sensor_data: Dict[str, Any]) -> bool:
    """
    Convenience function to upload sensor data to MQTT.
    
    Args:
        sensor_data: Dictionary containing sensor readings
        
    Returns:
        bool: True if published successfully, False otherwise
    """
    try:
        uploader = get_mqtt_uploader()
        return uploader.publish_sensor_data(sensor_data)
    except Exception as e:
        logging.error(f"❌ Error in upload_to_mqtt: {e}")
        return False
