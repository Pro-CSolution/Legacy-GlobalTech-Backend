# GlobalTech Backend – Producción en Windows (.exe embebido + Windows Service)

Este backend se ejecuta como **Servicio de Windows** y está pensado para distribuirse como un **.exe** con recursos embebidos (PyInstaller). La app sirve **FastAPI + Socket.IO** y lee configuración desde archivos `config/` y `parameters/` **incluidos dentro del bundle**.

## Qué se embebe (requiere rebuild si cambia)
- `config/devices.yaml`
- `config/drive_menus.json`
- `config/fault_codes.json`
- `parameters/*.json`
- `parameters.json` (fallback legacy)

## Endpoints de salud (operación)
- `GET /health/live`: liveness (si el proceso está vivo).
- `GET /health/ready`: readiness (503 hasta que la DB esté lista).
- `GET /health`: resumen (siempre 200; muestra degradación y causas).

## Variables de entorno (recomendadas en producción)

### Red / servidor
- `HOST` (default: `0.0.0.0`)
- `PORT` (default: `8000`)
- `ACCESS_LOG` (`1` para habilitar access logs de Uvicorn; default: `0`)

### Logging
- `LOG_LEVEL` (default: `INFO`)
- `LOG_TO_CONSOLE` (`1` para habilitar consola; default: `0` – recomendado para servicios)
- `GLOBALTECH_LOG_DIR` (o `LOG_DIR`) para forzar destino de logs.

**Destino por defecto de logs**:
- Servicio / ejecutable frozen: `%ProgramData%\GlobalTech\Backend\logs\backend.log`

### Base de datos (TimescaleDB / Postgres)
- `DATABASE_URL` (ejemplo):
  - `postgresql+asyncpg://user:password@<host>:5432/globaltech_db`

Notas:
- El servicio **no se apaga** si la DB no está lista. Reintenta en background.
- Hasta que la DB esté lista, endpoints que dependen de DB retornan **503** con detalles.

### Integraciones / comportamiento
- `RESEND_API_KEY` (requerido para `/api/v1/reports/trend-email`)
- `USE_MOCK_DATA` (`1` para usar datos simulados)
- `MODBUS_POLL_INTERVAL` (default: `0.35`)
- `DATA_LOG_INTERVAL` (default: `None` → usa `MODBUS_POLL_INTERVAL`)

## Servicio de Windows (pywin32)

El entrypoint del servicio es `service.py` (y el .exe resultante mantiene la misma interfaz de comandos).

Comandos típicos:
- `install`: instala el servicio
- `remove`: elimina el servicio
- `start`: inicia el servicio
- `stop`: detiene el servicio
- `restart`: reinicia el servicio
- `debug`: ejecuta en foreground (útil para pruebas, sin SCM)

Recomendaciones operativas:
- Para `install/remove` ejecuta con **Administrador**.
- Si el servicio “arranca y muere”, revisa primero el log en `%ProgramData%\GlobalTech\Backend\logs\`.

## Firewall / red
- Asegura regla de entrada para `TCP/<PORT>` (default 8000) en el host donde corre el servicio.
- La app abrirá conexiones salientes Modbus TCP a los hosts definidos en `config/devices.yaml`.

## Checklist antes de declarar producción
- El `.exe` responde `GET /health/live` y `GET /health`.
- `GET /health/ready` pasa a 200 cuando la DB está lista.
- Los logs se escriben en `%ProgramData%\GlobalTech\Backend\logs\backend.log`.
- El servicio `stop` no deja procesos colgados.
- Los recursos embebidos (`config/` + `parameters/`) se leen correctamente en modo frozen.


## Commands

```
cd "C:\Users\adria\OneDrive\Documents\Projects\GlobalTech HMI\GlobalTech-Backend"
.\venv\Scripts\pip.exe install -r requirements.txt
if (Test-Path dist) { Remove-Item -Recurse -Force dist }
if (Test-Path build) { Remove-Item -Recurse -Force build }
.\venv\Scripts\pyinstaller.exe .\globaltech_backend.spec
```

