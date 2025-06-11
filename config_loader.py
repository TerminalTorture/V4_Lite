# filepath: c:\\Users\\LeeJR\\Desktop\\VFlow\\Webpage\\V4\\api\\config_loader.py
import yaml
import os
from collections import defaultdict

CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'register_config.yaml') # Path relative to this file

def load_register_config():
    """Loads and processes the register_config.yaml configuration."""
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Configuration file not found: {CONFIG_PATH}")

    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        config_data = yaml.safe_load(f)

    if not config_data:
        raise ValueError("Configuration file is empty or invalid.")

    registers = config_data.get('registers', [])
    modbus_config = config_data.get('modbus', {}) # Load modbus section

    if not registers: # It's okay if registers are empty, but modbus section might be needed
        # Depending on requirements, you might want to raise an error if 'registers' is critical
        # For now, we'll allow it if 'modbus' section is present and valid
        pass


    # --- Process and index the configuration for easier access ---

    # Map name to register info
    registers_by_name = {reg['name']: reg for reg in registers}

    # Map address to register info
    registers_by_address = {reg['address']: reg for reg in registers}

    # Group registers by their 'group' key
    registers_by_group = defaultdict(list)
    for reg in registers:
        if 'group' in reg:
            registers_by_group[reg['group']].append(reg)

    # Group registers by their 'ui.view' key
    registers_by_view = defaultdict(list)
    for reg in registers:
        views = reg.get('ui', {}).get('view', [])
        if isinstance(views, str): # Handle single string view
            views = [views]
        for view in views:
            registers_by_view[view].append(reg)


    # Find the maximum address to determine read range (optional, can be refined)
    max_address = 0
    min_address = 0 # Initialize min_address
    if registers:
        addresses = [reg['address'] for reg in registers]
        max_address = max(addresses)
        min_address = min(addresses) # Calculate minimum address

    # Add buffer if needed, e.g., read one extra register if max address is 148
    # total_register_count = max_address + 1 if max_address > 0 else 0 # This might not be accurate if addresses are sparse
    # Calculate total count based on range from min to max
    total_register_count = (max_address - min_address + 1) if registers else 0

    # --- Return a structured dictionary ---
    return {
        'raw': registers,
        'by_name': registers_by_name,
        'by_address': registers_by_address,
        'by_group': dict(registers_by_group), # Convert defaultdict back to dict
        'by_view': dict(registers_by_view),   # Convert defaultdict back to dict
        'max_address': max_address,
        'min_address': min_address, # Add min_address
        'total_register_count': total_register_count, # Use the range-based count
        'modbus_ip': modbus_config.get('ip'), # Add Modbus IP
        'modbus_port': modbus_config.get('port') # Add Modbus Port
    }

# --- Load the configuration ONCE when the module is imported ---
REGISTER_CONFIG = load_register_config()

# --- Optional: Print loaded config details for verification ---
# print(f"✅ Register config loaded. Min Addr: {REGISTER_CONFIG['min_address']}, Max Addr: {REGISTER_CONFIG['max_address']}, Count: {REGISTER_CONFIG['total_register_count']}")
# print(f"   Registers by name: {list(REGISTER_CONFIG['by_name'].keys())}")
# print(f"   Registers by address: {list(REGISTER_CONFIG['by_address'].keys())}")
