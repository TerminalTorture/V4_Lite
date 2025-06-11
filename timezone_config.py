"""
Centralized timezone configuration for the entire application.
This module contains the timezone definition that should be imported by all other modules.
"""

from datetime import timezone, timedelta

# Define the application timezone - can be easily changed for different deployments
set_timezone = timezone(timedelta(hours=8), name='GMT+8') 