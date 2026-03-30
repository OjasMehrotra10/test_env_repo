import os
import time
import nipyapi

# -----------------------------
# Config from GitHub Secrets
# -----------------------------
NIFI_HOST = os.getenv("NIFI_HOST")
NIFI_TOKEN = os.getenv("NIFI_TOKEN")

# Example:
# NIFI_HOST = "https://nifi.yourdomain.com/nifi-api"

# -----------------------------
# Setup NiFi Client
# -----------------------------
nipyapi.config.nifi_config.host = NIFI_HOST

# Token-based auth (Openflow compatible)
nipyapi.security.service_login(service='nifi', token=NIFI_TOKEN)

print("Connected to NiFi")

# -----------------------------
# Trigger Flow
# -----------------------------
try:
    # Example: Start a process group
    PG_ID = "your-process-group-id"

    nipyapi.canvas.schedule_process_group(PG_ID, True)

    print(f"Process group {PG_ID} started successfully")

    # Optional wait
    time.sleep(5)

    # Stop if needed
    # nipyapi.canvas.schedule_process_group(PG_ID, False)

except Exception as e:
    print("Error triggering NiFi:", str(e))
    raise
