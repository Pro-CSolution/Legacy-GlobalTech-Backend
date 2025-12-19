#!/usr/bin/env python3
"""
Script to add scale_factor property to all drive parameters in drive.parameters.json

Based on the analysis provided, most parameters use scale_factor = 100 (divide by 100 when reading, multiply by 100 when writing).
Exceptions are determined by how values are displayed in the drive interface.
"""

import json
import os
from typing import Dict, Any

# Scale factors based on the detailed analysis
SCALE_FACTORS = {
    # Menu 1
    "P1.00": 100,  # (0.00… 650.00)
    "P1.01": 100,  # (0.00… 150.00)
    "P1.02": 10,   # (0.0… 1000.0)
    "P1.03": 100,  # (0.00… 650.00)
    "P1.04": 1,    # (entero 0…9999)
    "P1.05": 10,   # (0.0… 1000.0)
    "P1.12": 10,   # (0.0… 1000.0 A)
    "P1.13": 1,    # (entero RPM)
    "P1.14": 100,  # (0.10… 0.99)
    "P1.15": 1,    # (entero RPM)
    "P1.16": 1,    # (entero RPM)
    "P1.17": 1,    # (entero RPM)
    "P1.18": 1,    # (entero RPM)

    # Menu 2
    "P2.01": 1,    # (entero V)
    "P2.02": 10,   # (0.0… 1000.0 A)
    "P2.03": 1,    # (0…9999)

    # Menu 5
    "P5.00": 100,  # (% of top speed)
    "P5.14": 10,   # (0.0… 100.0 %)
    "P5.22": 1,    # (entero -9999…9999)

    # Menu 8
    "P8.00": 10,   # (0.0… 300.0 %)

    # Menu 9 (monitoring)
    "P9.04": 10,   # (-300.0…300.0%)
    "P9.05": 10,   # (0.0…9999.9 A)
    "P9.06": 10,   # (0.0…300.0%)
    "P9.08": 10,   # (-999.0…+999.9)

    # Menu 10 (warnings/trips/history) - codes and counters = 1
    "P10.00": 1,   # Warning Code
    "P10.01": 1,   # Warning Code
    "P10.02": 1,   # Warning Code
    "P10.10": 1,   # Trip Code
    "P10.11": 1,   # Trip Code
    "P10.12": 1,   # Trip Code
    "P10.13": 1,   # Trip Code
    "P10.14": 1,   # Trip Code
    "P10.20": 1,   # Trip History Code
    "P10.21": 1,   # Trip History Code
    "P10.22": 1,   # Trip History Code
    "P10.23": 1,   # Trip History Code
    "P10.24": 1,   # Trip History Code
    "P10.25": 1,   # Trip History Code
    "P10.26": 1,   # Trip History Code
    "P10.27": 1,   # Trip History Code
    "P10.28": 1,   # Trip History Code
    "P10.29": 1,   # Trip History Code
    "P10.30": 1,   # Trip History Seconds
    "P10.31": 1,   # Trip History Hours
    "P10.34": 1000,  # Control Flag Value (1.006)

    # Menu 11 (monitoring)
    "P11.00": 10,  # (-150.0…+150.0%)
    "P11.01": 10,  # (-150.0…+150.0%)
    "P11.03": 1,   # (0…650 V)
    "P11.04": 10,  # (0.0…9999.9 A)
    "P11.05": 10,  # (0.0…9999.9 V)
    "P11.06": 10,  # (0.0…150.0 Hz)
    "P11.07": 10,  # (0.0…1999.9%)
    "P11.08": 10,  # (-50.0…+150.0 °C)
    "P11.09": 10,  # (-300.0…+300.0%)
    "P11.10": 10,  # (-50.0…+150.0 °C)
    "P11.11": 10,  # (-50.0…+150.0 °C)
    "P11.19": 1,   # kW-Hours
    "P11.20": 1,   # MW-Hours
    "P11.49": 100, # (0.00…300.00%)
    "P11.50": 100, # (-100.00…+100.00%)
}

def determine_scale_factor(param: Dict[str, Any]) -> int:
    """
    Determine the scale factor for a parameter based on its properties.

    Args:
        param: Parameter dictionary

    Returns:
        Scale factor (1, 10, 100, or 1000)
    """
    param_id = param.get("id", "")

    # Check if we have a specific scale factor for this parameter
    if param_id in SCALE_FACTORS:
        return SCALE_FACTORS[param_id]

    # Default logic for parameters not explicitly listed
    # Most parameters use scale = 100 (shown with 2 decimals)
    # But we need to be careful with codes, counters, and certain units

    unit = (param.get("unit") or "").lower()
    default = param.get("default", "")
    name = (param.get("name") or "").lower()

    # Codes and counters typically have scale = 1
    if any(keyword in name for keyword in ["code", "counter", "warning", "trip", "history", "flag"]):
        return 1

    # Voltage parameters that are integers (like P2.01, P11.03)
    if unit == "v" and default.isdigit():
        return 1

    # RPM parameters that are integers
    if unit == "rpm" and default.isdigit():
        return 1

    # kW-Hours and MW-Hours are integers
    if "hour" in unit.lower():
        return 1

    # Default to 100 for most parameters (shown with 2 decimals)
    return 100

def add_scale_factors():
    """Add scale_factor property to all parameters in drive.parameters.json"""

    # Path to the parameters file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    params_file = os.path.join(script_dir, "..", "parameters", "drive.parameters.json")

    print(f"Reading parameters from: {params_file}")

    # Read the JSON file
    try:
        with open(params_file, 'r', encoding='utf-8') as f:
            parameters = json.load(f)
    except FileNotFoundError:
        print(f"Error: File {params_file} not found")
        return
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return

    print(f"Found {len(parameters)} parameters")

    # Track scale factor usage
    scale_count = {1: 0, 10: 0, 100: 0, 1000: 0}

    # Add scale_factor to each parameter
    for param in parameters:
        scale_factor = determine_scale_factor(param)
        param["scale_factor"] = scale_factor
        scale_count[scale_factor] += 1

    print("Scale factor distribution:")
    for scale, count in scale_count.items():
        print(f"  Scale {scale}: {count} parameters")

    # Write back to file
    try:
        with open(params_file, 'w', encoding='utf-8') as f:
            json.dump(parameters, f, indent=2, ensure_ascii=False)
        print(f"Successfully updated {params_file}")
    except Exception as e:
        print(f"Error writing file: {e}")

if __name__ == "__main__":
    add_scale_factors()
