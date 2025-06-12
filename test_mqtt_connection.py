import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Ensure the mqtt_client module can be found
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from mqtt_client import MQTTUploader

# Setup basic logging for the test
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_connection():
    """
    Tests the MQTT connection using MQTTUploader.
    """
    logging.info("Attempting to initialize MQTTUploader...")
    try:
        # Initialize MQTTUploader (it will load config from .env)
        uploader = MQTTUploader()
    except Exception as e:
        logging.error(f"Failed to initialize MQTTUploader: {e}")
        return

    if not uploader.enabled:
        logging.info("MQTT publishing is disabled in the configuration. Test cannot proceed.")
        return

    logging.info(f"Attempting to connect to MQTT broker: {uploader.broker_host}:{uploader.broker_port} "
                 f"with transport {uploader.transport} "
                 f"(TLS: {uploader.use_tls}, WS Path: {uploader.ws_path if uploader.transport == 'websockets' else 'N/A'})")

    if uploader.connect():
        logging.info("âœ… Successfully connected to MQTT broker.")
        logging.info("Attempting to publish a test status message...")
        if uploader.publish_status_message("online", "Connection test successful"):
            logging.info("âœ… Test status message published successfully.")
        else:
            logging.error("âŒ Failed to publish test status message.")
        uploader.disconnect()
        logging.info("âœ… Disconnected from MQTT broker.")
    else:
        logging.error("âŒ Failed to connect to MQTT broker.")
        # Attempt to disconnect even if connection failed, to clean up resources
        uploader.disconnect()

if __name__ == "__main__":
    test_connection()