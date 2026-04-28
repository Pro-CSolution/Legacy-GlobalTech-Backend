"""
Inserta parámetros del Wago en archivos dedicados (uno por device) con address y register_type.

Mapeo de tipos:
- AI -> input          (función 4)
- AO -> holding        (función 3/6)
- DI -> discrete       (función 2)
- DO -> coil           (función 1/5)
"""

import json
from pathlib import Path


def map_register_type(tag_type: str) -> str:
    if not tag_type:
        return "holding"
    tag_type = tag_type.strip().upper()
    if tag_type == "AI":
        return "input"
    if tag_type == "AO":
        return "holding"
    if tag_type == "DI":
        return "discrete"
    if tag_type == "DO":
        return "coil"
    return "holding"


# Tabla manual (resumen del Excel)
WAGO_PARAMS = [
    ("Remote_Speed_In", "Test Stand A Remote SPD IN", "AI", 0, 1, "Raw", "4-20mA", "750-476"),
    ("Auto_Speed_SP1", "Test Stand A Analog In SP 1", "AI", 1, 1, "Raw", "4-20mA", "750-476"),
    ("Cooling_Water_Press", "Converter Cooling Water Pressure", "AI", 2, 1, "Raw", "4-20mA", "750-476"),
    ("Spare_AI_03", "Spare", "AI", 3, 1, "Raw", "0-20mA", "750-476"),
    ("Speed_Out_Drive", "Speed Ref to Drive", "AO", 0, 1, "Raw", "0-20mA", "750-552"),
    ("Spare_AO_01", "Spare", "AO", 1, 1, "Raw", "0-20mA", "750-552"),
    ("Spare_AO_02", "Spare", "AO", 2, 1, "Raw", "0-10VDC", "750-552"),
    ("Spare_AO_03", "Spare", "AO", 3, 1, "Raw", "0-10VDC", "750-552"),
    ("Temp_Winding_A1", "Test Motor A Temp Winding A1", "AI", 4, 1, "Raw", "PT100", "750-461"),
    ("Temp_Winding_B1", "Test Motor A Temp Winding B1", "AI", 5, 1, "Raw", "PT100", "750-461"),
    ("Temp_Winding_C1", "Test Motor A Temp Winding C1", "AI", 6, 1, "Raw", "PT100", "750-461"),
    ("Temp_Winding_A2", "Test Motor A Temp Winding A2", "AI", 7, 1, "Raw", "PT100", "750-461"),
    ("Temp_Winding_B2", "Test Motor A Temp Winding B2", "AI", 8, 1, "Raw", "PT100", "750-461"),
    ("Temp_Winding_C2", "Test Motor A Temp Winding C2", "AI", 9, 1, "Raw", "PT100", "750-461"),
    ("Temp_Bearing_DE", "Test Motor A Temp Bearing Drive End", "AI", 10, 1, "Raw", "PT100", "750-461"),
    ("Temp_Bearing_NDE", "Test Motor A Temp Bearing Non-Drive End", "AI", 11, 1, "Raw", "PT100", "750-461"),
    ("Spare_AI_12", "Test Motor A Spare", "AI", 12, 1, "Raw", "PT100", "750-461"),
    ("Spare_AI_13", "Test Motor A Spare", "AI", 13, 1, "Raw", "PT100", "750-461"),
    ("Spare_AI_14", "Test Motor A Spare", "AI", 14, 1, "Raw", "PT100", "750-461"),
    ("Spare_AI_15", "Test Motor A Spare", "AI", 15, 1, "Raw", "PT100", "750-461"),
    ("Spare_AI_16", "Test Motor A Spare", "AI", 16, 1, "Raw", "PT100", "750-461"),
    ("Spare_AI_17", "Test Motor A Spare", "AI", 17, 1, "Raw", "PT100", "750-461"),
    ("Spare_AI_18", "Test Motor A Spare", "AI", 18, 1, "Raw", "PT100", "750-461"),
    ("Spare_AI_19", "Test Motor A Spare", "AI", 19, 1, "Raw", "PT100", "750-461"),
    ("Drive_Cooling_Temp", "Drive Cooling Water Temp", "AI", 20, 1, "Raw", "PT100", "750-461"),
    ("Spare_AI_21", "Spare", "AI", 21, 1, "Raw", "PT100", "750-461"),
    ("Spare_AI_22", "Spare", "AI", 22, 1, "Raw", "PT100", "750-461"),
    ("Spare_AI_23", "Spare", "AI", 23, 1, "Raw", "PT100", "750-461"),
    ("Spare_AI_24", "Spare", "AI", 24, 1, "Raw", "PT100", "750-461"),
    ("Spare_AI_25", "Spare", "AI", 25, 1, "Raw", "PT100", "750-461"),
    ("RB_Local_Control", "Local Control", "DI", 3, 1, "Bool", "0-1", "750-421"),
    ("RB_Remote_Control", "Remote Control", "DI", 4, 1, "Bool", "0-1", "750-421"),
    ("RB_Breaker_Close", "Breaker Close", "DI", 7, 1, "Bool", "0-1", "750-421"),
    ("RB_Breaker_Open", "Breaker Open", "DI", 8, 1, "Bool", "0-1", "750-421"),
    ("RB_Estop_Status", "E-Stop Status", "DI", 11, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_12", "Spare", "DI", 12, 1, "Bool", "0-1", "750-421"),
    ("Relay_Pump_Brk_Closed", "Relay Pump Breaker Closed", "DI", 15, 1, "Bool", "0-1", "750-421"),
    ("WF_Breaker_Closed", "Spare", "DI", 16, 1, "Bool", "0-1", "750-421"),
    ("Drive_Coolant_Leak", "Spare", "DI", 19, 1, "Bool", "0-1", "750-421"),
    ("Supply_480VAC_On", "480VAC Supply ON", "DI", 20, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_23", "Spare", "DI", 23, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_24", "Spare", "DI", 24, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_27", "Spare", "DI", 27, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_28", "Spare", "DI", 28, 1, "Bool", "0-1", "750-421"),
    ("Drive_Coolant_Pump2_Running", "Drive_Coolant_Pump2_Running", "DI", 31, 1, "Bool", "0-1", "750-421"),
    ("Drive_Coolant_Pump_Hand", "Drive_Coolant_Pump_Hand", "DI", 32, 1, "Bool", "0-1", "750-421"),
    ("Drive_Cooland Pump_Auto", "Drive_Cooland Pump_Auto", "DI", 35, 1, "Bool", "0-1", "750-421"),
    ("Drive_Coolant_Pump1_Running", "Drive_Coolant_Pump1_Running", "DI", 36, 1, "Bool", "0-1", "750-421"),
    ("Drive_Coolant_Flow_Sw", "Drive_Coolant_Flow_Sw", "DI", 39, 1, "Bool", "0-1", "750-421"),
    ("Drive_Coolant_Pressure_Sw", "Drive_Coolant_Pressure_Sw", "DI", 40, 1, "Bool", "0-1", "750-421"),
    ("Drive_Coolant Leak_Sw", "Drive_Coolant Leak_Sw", "DI", 43, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_44", "Spare", "DI", 44, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_47", "Spare", "DI", 47, 1, "Bool", "0-1", "750-421"),
    ("Drive_Coolant_Pump_Sw", "Drive_Coolant_Pump_Sw", "DI", 48, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_51", "Spare", "DI", 51, 1, "Bool", "0-1", "750-421"),
    ("RB_Blower_Run_Status", "Blower Motor Run Status", "DI", 52, 1, "Bool", "0-1", "750-421"),
    ("RB_Blower_Press_Sw", "Motor Blower Pressure Switch", "DI", 55, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_56", "Spare", "DI", 56, 1, "Bool", "0-1", "750-421"),
    ("RB_Drive_OK", "DO1 Drive OK", "DI", 59, 1, "Bool", "0-1", "750-421"),
    ("RB_Drive_Running", "Drive DO2 Drive Running", "DI", 60, 1, "Bool", "0-1", "750-421"),
    ("RB_Precharge_OK", "Drive DO3 Precharge Complete", "DI", 63, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_64", "Spare", "DI", 64, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_67", "Spare", "DI", 67, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_68", "Spare", "DI", 68, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_71", "Spare", "DI", 71, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_72", "Spare", "DI", 72, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_75", "Spare", "DI", 75, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_76", "Spare", "DI", 76, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_79", "Spare", "DI", 79, 1, "Bool", "0-1", "750-421"),
    ("Spare_DI_80", "Spare", "DI", 80, 1, "Bool", "0-1", "750-421"),
    ("Coolant_Pump1_Run", "Coolant_Pump1_Run", "DO", 0, 1, "Bool", "0-1", "750-600"),
    ("Trans_Cooling_Start", "Transformer Cooling Fan Start", "DO", 1, 1, "Bool", "0-1", "750-600"),
    ("Spare_DO_02", "Spare (Waste Fines E-Stop Reset)", "DO", 2, 1, "Bool", "0-1", "750-600"),
    ("Spare_DO_03", "Spare", "DO", 3, 1, "Bool", "0-1", "750-600"),
    ("Motor_Heater_Ctrl", "Motor Heater Control Relay (DRV Running)", "DO", 4, 1, "Bool", "0-1", "750-600"),
    ("RB_Main_Brk_Open", "Main Breaker Open", "DO", 5, 1, "Bool", "0-1", "750-600"),
    ("Spare_DO_06", "Spare", "DO", 6, 1, "Bool", "0-1", "750-600"),
    ("RB_Estop_Reset", "Relay Pump E-Stop Reset", "DO", 7, 1, "Bool", "0-1", "750-600"),
    ("RB_Blower_Start", "Motor Blower Start", "DO", 9, 1, "Bool", "0-1", "750-600"),
    ("Spare_DO_09", "Spare", "DO", 9, 1, "Bool", "0-1", "750-600"),
    ("Spare_DO_10", "Spare", "DO", 10, 1, "Bool", "0-1", "750-600"),
    ("Spare_DO_11", "Spare", "DO", 11, 1, "Bool", "0-1", "750-600"),
    ("RB_Main_Brk_Close", "Relay Pump Main Breaker Close", "DO", 12, 1, "Bool", "0-1", "750-600"),
    ("Precharge_Trans_PC1", "Precharge Transformer PC1", "DO", 13, 1, "Bool", "0-1", "750-600"),
    ("RB_Drive_Precharge", "Relay Pump Drive Precharge PC2", "DO", 14, 1, "Bool", "0-1", "750-600"),
    ("Spare_DO_15", "Spare", "DO", 15, 1, "Bool", "0-1", "750-600"),
    ("Spare_DO_16", "Spare", "DO", 16, 1, "Bool", "0-1", "750-600"),
    ("Coolant_Pump2_Run", "Coolant_Pump2_Run", "DO", 17, 1, "Bool", "0-1", "750-600"),
    ("Spare_DO_18", "Spare (Main CB Open Light)", "DO", 18, 1, "Bool", "0-1", "750-600"),
    ("Main_CB_Closed_Light", "Main CB Closed Light", "DO", 19, 1, "Bool", "0-1", "750-600"),
    ("RB_Drive_Start", "Drive Start", "DO", 21, 1, "Bool", "0-1", "750-600"),
    ("RB_Drive_Reset", "Drive Reset", "DO", 20, 1, "Bool", "0-1", "750-600"),
    ("Spare_DO_22", "Drive (Spare)", "DO", 22, 1, "Bool", "0-1", "750-600"),
    ("Spare_DO_23", "Spare", "DO", 23, 1, "Bool", "0-1", "750-600"),
    ("Spare_DO_24", "Spare (Waste Fines Pump Drive Start)", "DO", 24, 1, "Bool", "0-1", "750-600"),
    ("Spare_DO_25", "Spare (Waste Fines Pump Drive Reset)", "DO", 25, 1, "Bool", "0-1", "750-600"),
    ("Spare_DO_26", "Spare (Waste Fines Pump Drive Spare)", "DO", 26, 1, "Bool", "0-1", "750-600"),
    ("Spare_DO_27", "Spare", "DO", 27, 1, "Bool", "0-1", "750-600"),
    ("Spare_DO_28", "Spare", "DO", 28, 1, "Bool", "0-1", "750-600"),
    ("Trans_Cooling_Ctrl", "Transformer Cooling Fans Control Relay", "DO", 29, 1, "Bool", "0-1", "750-600"),
]


def main():
    root = Path(__file__).resolve().parents[1]
    output_files = [
        root / "parameters" / "wago_a.parameters.json",
        root / "parameters" / "wago_b.parameters.json",
    ]

    # Lista de tags críticos a insertar (puedes editarla)
    important_tags = {
        # AI
        "Remote_Speed_In",
        "Auto_Speed_SP1",
        "Cooling_Water_Press",
        "Drive_Cooling_Temp",
        "Temp_Winding_A1",
        "Temp_Winding_B1",
        "Temp_Winding_C1",
        "Temp_Bearing_DE",
        "Temp_Bearing_NDE",
        # AO
        "Speed_Out_Drive",
        # DI
        "RB_Local_Control",
        "RB_Remote_Control",
        "RB_Breaker_Close",
        "RB_Breaker_Open",
        "RB_Estop_Status",
        "Supply_480VAC_On",
        "Drive_Coolant_Flow_Sw",
        "Drive_Coolant_Pressure_Sw",
        "Drive_Coolant Leak_Sw",
        "Drive_Coolant_Pump_Sw",
        "Drive_Coolant_Pump2_Running",
        "Drive_Coolant_Pump1_Running",
        "Drive_Cooland Pump_Auto",
        "RB_Drive_OK",
        "RB_Drive_Running",
        "RB_Precharge_OK",
        # DO
        "Coolant_Pump1_Run",
        "Trans_Cooling_Start",
        "Motor_Heater_Ctrl",
        "RB_Estop_Reset",
        "RB_Blower_Start",
        "RB_Main_Brk_Close",
        "RB_Main_Brk_Open",
        "RB_Drive_Start",
        "RB_Drive_Reset",
        "Main_CB_Closed_Light",
        "Coolant_Pump2_Run",
    }

    for params_path in output_files:
        if params_path.exists():
            data = json.loads(params_path.read_text(encoding="utf-8"))
        else:
            data = []

        existing_ids = {p.get("id") for p in data}
        added = 0
        skipped = 0

        for tag, desc, ttype, addr, scale, unit, range_txt, module in WAGO_PARAMS:
            if tag not in important_tags:
                continue
            if tag in existing_ids:
                skipped += 1
                continue
            param = {
                "id": tag,
                "name": desc,
                "description": desc,
                "address": addr,
                "register_type": map_register_type(ttype),
                "scale_factor": scale,
                "unit": unit,
                "range_text": range_txt,
                "module": module,
                "attributes": ["R"] if map_register_type(ttype) in {"input", "discrete"} else ["R", "W"],
            }
            data.append(param)
            added += 1

        params_path.parent.mkdir(parents=True, exist_ok=True)
        params_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"{params_path.name}: agregados {added}, ya existentes {skipped}")


if __name__ == "__main__":
    main()

