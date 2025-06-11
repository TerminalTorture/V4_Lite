import os
import sys
import threading
import subprocess
from flask import Flask, render_template, redirect, url_for, send_from_directory, request, jsonify, flash
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from config_loader import REGISTER_CONFIG # Import the loaded config
from modbus_client import read_modbus_data
from datetime import datetime, timedelta, timezone # MODIFIED: timezone import already present, UTC removed as gmt_plus_8 will be used
from timezone_config import set_timezone # ADDED: Import set_timezone
from mqtt_client import upload_to_mqtt as upload_to_cloud
import logging # Add logging import
import yaml # Add this import

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your_secret_key'

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Add API endpoint for register definitions ---
@app.route('/api/registers/definitions')
def get_register_definitions():
    # Return the processed configuration relevant for the frontend
    # Filter or structure as needed, here returning groups and raw list
    definitions = {
        "registers": REGISTER_CONFIG.get("raw", []),
        "groups": REGISTER_CONFIG.get("by_group", {}),
        "views": REGISTER_CONFIG.get("by_view", {})
        # Add other processed parts of the config if needed by frontend
    }
    return jsonify(definitions)
# --- Endpoint added ---

# ... (Background task setup)
collect_sensor_lock = threading.Lock()

def process_and_upload_sensor_data():
    """Read Modbus data, process it using register_config.yaml, and upload to MQTT."""
    if not collect_sensor_lock.acquire(blocking=False):
        logging.warning("Could not acquire sensor data lock, process already running.")
        return

    try:
        logging.debug("Reading Modbus data and processing for MQTT upload...")
        
        # Read fresh Modbus data
        raw_data = read_modbus_data()
        if not raw_data:
            logging.warning("No Modbus data available for processing")
            return

        # Process data with scaling (similar to live_data.py)
        processed_data = {}
        current_time_app_tz = datetime.now(set_timezone)
        
        for name, raw_value in raw_data.items():
            if raw_value is None:
                processed_data[name] = None
                continue

            reg_info = REGISTER_CONFIG['by_name'].get(name)
            if reg_info:
                # Apply scaling if defined
                scale = reg_info.get('scale')
                if scale is not None:
                    try:
                        processed_value = float(raw_value) * float(scale)
                    except (ValueError, TypeError):
                        logging.warning(f"Could not apply scale to {name}: {raw_value}")
                        processed_value = raw_value
                else:
                    processed_value = raw_value
                processed_data[name] = processed_value
            else:
                processed_data[name] = raw_value

        # Add timestamp and device ID
        processed_data['timestamp'] = current_time_app_tz.isoformat()
        device_id = os.getenv('CLOUD_DEVICE_ID', 'vflow_sensor_client')
        processed_data['device_id'] = device_id

        # Upload to MQTT
        upload_success = upload_to_cloud(processed_data)
        if upload_success:
            logging.debug("📤 Data processed and uploaded to MQTT successfully")
        else:
            logging.warning("⚠️ Failed to upload processed data to MQTT")
                
    except Exception as e:
        logging.error(f"Error in process_and_upload_sensor_data: {e}", exc_info=True)
    finally:
        collect_sensor_lock.release()

# ... (Scheduler setup remains the same)
executors = {
    'default': ThreadPoolExecutor(1) # Only allow 1 thread at a time
}

def start_scheduler():
    # Get data collection and upload interval from environment variable (default: 5 seconds)
    upload_interval = int(os.getenv('DATA_UPLOAD_INTERVAL', '5'))
    
    scheduler = BackgroundScheduler(executors=executors)
    # Schedule data processing and MQTT upload (configurable interval)
    # Change DATA_UPLOAD_INTERVAL in .env file to modify frequency
    scheduler.add_job(process_and_upload_sensor_data, 'interval', seconds=upload_interval, id='process_upload_job', replace_existing=True)
    scheduler.start()
    logging.info(f"Background scheduler started with data processing and MQTT upload every {upload_interval}s.")

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static', 'images'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/')
def index():
    # Add current_time for cache busting static files
    current_time_val = datetime.now(set_timezone).timestamp()
    return render_template('index.html', current_time=current_time_val)

@app.route('/settings')
def settings():
    # Get Modbus config from REGISTER_CONFIG, which loads from register_config.yaml
    # Provide defaults in case the keys are missing in the loaded config
    modbus_ip = REGISTER_CONFIG.get('modbus_ip', "192.168.1.25")
    modbus_port = REGISTER_CONFIG.get('modbus_port', 1502) # Ensure it's treated as int if needed by template
    return render_template('settings.html', modbus_ip=modbus_ip, modbus_port=modbus_port)

@app.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)

@app.route('/api/set-modbus-config', methods=['POST'])
def set_modbus_config():
    data = request.json
    ip = data.get('ip')
    port = data.get('port')

    if not ip or not port:
        return jsonify({"error": "Missing IP or Port"}), 400

    config_path = os.path.join(app.root_path, 'register_config.yaml')

    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)

        if 'modbus' not in config_data or not isinstance(config_data['modbus'], dict):
            config_data['modbus'] = {} # Ensure modbus section exists

        config_data['modbus']['ip'] = ip
        config_data['modbus']['port'] = int(port) # Ensure port is an integer

        with open(config_path, 'w') as f:
            yaml.dump(config_data, f, sort_keys=False)

        logging.info(f"Updated Modbus config in register_config.yaml: IP={ip}, Port={port}")
        flash("Modbus configuration updated in register_config.yaml. Please restart the application for changes to take full effect.", "success")
        return jsonify({"message": "Modbus configuration updated in register_config.yaml. A restart is likely required."})
    except Exception as e:
        logging.error(f"Error updating Modbus config in {config_path}: {e}", exc_info=True)
        flash(f"Error updating Modbus configuration file: {e}", "error")
        return jsonify({"error": f"Failed to update configuration file: {e}"}), 500

if __name__ == '__main__':
    start_scheduler()
    # Use host='0.0.0.0' to make it accessible on the network
    app.run(debug=True, host='0.0.0.0', port=5000) # Added host and port


