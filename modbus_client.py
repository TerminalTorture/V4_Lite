import os
import sys
from pymodbus.client import ModbusTcpClient
from datetime import datetime
from pymodbus.exceptions import ConnectionException
import logging # Import logging

# Calculate the project root (one directory up from the current file)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- Use the new config loader ---
from config_loader import REGISTER_CONFIG
# from variable_mapping import VARIABLE_MAPPING # No longer needed directly here


MODBUS_IP = REGISTER_CONFIG.get('modbus_ip', "192.168.0.33") # Get from loaded config
MODBUS_PORT = int(REGISTER_CONFIG.get('modbus_port', 1502)) # Get from loaded config

# --- Get read parameters from loaded config ---
# TOTAL_REGISTER_COUNT = int(os.getenv("MODBUS_VARIABLE_COUNT", "156")) # Replaced by config
# TOTAL_REGISTER_COUNT = REGISTER_CONFIG['total_register_count'] # Might not be needed directly here
MAX_ADDRESS_TO_READ = REGISTER_CONFIG['max_address'] # Useful for knowing the highest address needed
MIN_ADDRESS_TO_READ = REGISTER_CONFIG['min_address'] # Get the minimum address

# Max registers per read request (adjust if needed, 125 is a common limit)
MAX_READ_COUNT = 64

def get_modbus_client():
    """Use the globally defined Modbus configuration."""
    #print(f"Attempting Modbus connection to {MODBUS_IP}:{MODBUS_PORT}") # Use global constants if uncommented
    return ModbusTcpClient(MODBUS_IP, port=MODBUS_PORT) # Use global constants

def read_modbus_data():
    all_registers = {} # Use a dictionary to store registers by address
    client = None # Initialize client to None

    try:
        client = get_modbus_client()
        if not client.connect():
             print(f"❌ Failed to connect to Modbus server at {MODBUS_IP}:{MODBUS_PORT}")
             raise ConnectionException(f"Failed to connect to {MODBUS_IP}:{MODBUS_PORT}")
        else:
             pass # Connection successful

        # --- Read all necessary registers based on MIN/MAX_ADDRESS_TO_READ ---
        # We need to read from the lowest defined address up to the highest defined address.

        start_address = MIN_ADDRESS_TO_READ # Starts at 28 based on config
        registers_read = {}
        highest_address_needed = MAX_ADDRESS_TO_READ

        while start_address <= highest_address_needed:
             count = min(MAX_READ_COUNT, highest_address_needed - start_address + 1)
             if count <= 0: break

             # Ensure we are using HOLDING registers
             #logging.info(f"Attempting Modbus read: Function=read_holding_registers, Start Address={start_address}, Count={count}")
             # *** Use read_holding_registers ***
             response = client.read_holding_registers(start_address, count=count)

             if response.isError():
                 exception_code = getattr(response, 'exception_code', None)
                 logging.error(f"❌ Modbus Error: Function=read_holding_registers, Start Address={start_address}, Count={count} -> Response={response} (Code: {exception_code})")
                 raise Exception(f"Modbus read error at address {start_address}. Device responded with exception code: {exception_code}. Response details: {response}")

             if not hasattr(response, 'registers') or not response.registers:
                 logging.warning(f"⚠️ Modbus Warning: No registers returned or attribute missing for address {start_address} (count {count}). Response: {response}")
                 pass # Continue if no registers returned
             else:
                 # Store the read registers with their correct addresses
                 for i, value in enumerate(response.registers):
                     current_address = start_address + i
                     registers_read[current_address] = value
                     # logging.debug(f"  Read Addr {current_address}: {value}") # Use debug level

             # Move to the next chunk
             start_address += count


        # --- Map register values to variable names using REGISTER_CONFIG ---
        data_dict = {}
        # print("Mapping registers to variable names using REGISTER_CONFIG...")
        for address, reg_info in REGISTER_CONFIG['by_address'].items():
            if address in registers_read:
                raw_value = registers_read[address]
                data_type = reg_info.get('dataType') # Get the dataType from config

                # Convert to signed 16-bit integer if dataType indicates it
                # Ensure your YAML config uses one of these dataType strings for signed registers
                signed_types = ['int16', 'sint16', 'signed', 'short', 'signed_short', 'signed int16']
                if data_type and data_type.lower() in signed_types:
                    if raw_value > 32767:  # Max positive for 16-bit signed (2^15 - 1)
                        # Convert from two's complement
                        processed_value = raw_value - 65536 # 2^16
                    else:
                        processed_value = raw_value
                else:
                    # For other types (e.g., 'uint16', 'float', or if dataType is None/unspecified),
                    # pass the raw value. Scaling for floats is handled in live_data.py.
                    # Note: 32-bit floats/integers would require reading two registers and combining them,
                    # which is a more complex case not covered by this specific change.
                    processed_value = raw_value

                data_dict[reg_info['name']] = processed_value
            else:
                # Handle cases where a defined register wasn't read (e.g., read error, address gap)
                print(f"⚠️ Warning: Register '{reg_info['name']}' (Address {address}) defined but not found in read data.")
                data_dict[reg_info['name']] = None # Or some other indicator

        return data_dict

    except ConnectionException as ce:
        print(f"❌ Modbus Connection Error: {ce}")
        return {}
    except Exception as e:
        # Log the full error for better diagnosis
        import traceback
        print(f"❌ Modbus Error in read_modbus_data: {e}")
        # print(traceback.format_exc()) # Optional: print full traceback
        return {}
    finally:
        if client and client.is_socket_open():
            client.close()
            #print("Modbus connection closed.") # Confirm closure

# Optional: Function to generate variable_mapping.py if still needed elsewhere
def generate_variable_mapping_file():
    filepath = os.path.join(os.path.dirname(__file__), 'variable_mapping.py')
    # Use f-string for the whole content block for easier formatting
    content = f"""# variable_mapping.py
# Auto-generated from registers.yaml by modbus_client.py
# DO NOT EDIT MANUALLY - Changes will be overwritten

VARIABLE_MAPPING = {{
"""
    # Correctly indent and format each line within the f-string block
    for address, reg_info in REGISTER_CONFIG['by_address'].items():
        content += f"    {address}: '{reg_info['name']}',\\n" # Add comma and newline
    content += f"}}""" # Close the dictionary curly brace

    try:
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"✅ Successfully generated {filepath}")
    except Exception as e:
        print(f"❌ Failed to generate {filepath}: {e}")

#Example: Generate the file when this script is run directly
if __name__ == '__main__':
    get_modbus_client() # Test connection
    read_modbus_data() # Test read
#     generate_variable_mapping_file()
