import requests

"""
CONFIG
"""
NIFI_HOST   = "https://of--rbfuedq-wbc50273.snowflakecomputing.app/auditoriaruntime/nifi-api"
COOKIE      = """"""
ORIGIN      = "https://of--rbfuedq-wbc50273.snowflakecomputing.app"
GITHUB_PAT  = "".strip()
REPO_OWNER  = "OjasMehrotra10"
REPO_NAME   = "test_env_repo"
BRANCH      = "main"
CLIENT_NAME = "GitHubFlowRegistryClient"

HEADERS = {
    "Content-Type": "application/json",
    "Cookie": COOKIE,
    "Origin": ORIGIN,
    "Referer": f"{ORIGIN}/auditoriaruntime/nifi/",
}

"""
REGISTRY
"""
def list_registry_types():
    r = requests.get(f"{NIFI_HOST}/controller/registry-types", headers=HEADERS, verify=True)
    print(f"\n[registry-types] {r.status_code}")
    for t in r.json().get("flowRegistryClientTypes", []):
        print(f"  {t['type']}")

def list_registry_clients():
    r = requests.get(f"{NIFI_HOST}/controller/registry-clients", headers=HEADERS, verify=True)
    print(f"\n[registry-clients] {r.status_code}")
    for reg in r.json().get("registries", []):
        c = reg["component"]
        print(f"  {reg['id']}  {c['name']}  {c['type']}")
    return r.json().get("registries", [])

def client_exists(name):
    return any(r["component"]["name"] == name for r in list_registry_clients())

def create_github_registry():
    if client_exists(CLIENT_NAME):
        print(f"\n⚠️  '{CLIENT_NAME}' already exists — skipping.")
        return
    body = {
        "revision": {"clientId": "python-script", "version": 0},
        "disconnectedNodeAcknowledged": False,
        "component": {
            "name": CLIENT_NAME,
            "type": "org.apache.nifi.github.GitHubFlowRegistryClient",
            "description": "GitHub registry for CI/CD",
            "properties": {
                "GitHub API URL": "https://api.github.com/",
                "Repository Owner": REPO_OWNER,
                "Repository Name": REPO_NAME,
                "Authentication Type": "PERSONAL_ACCESS_TOKEN",
                "Personal Access Token": GITHUB_PAT,
                "Default Branch": BRANCH,
            }
        }
    }
    r = requests.post(f"{NIFI_HOST}/controller/registry-clients", headers=HEADERS, json=body, verify=True)
    print(f"\n[create-registry] {r.status_code}")
    if r.ok:
        d = r.json()
        print(f"✅ Registry Created — ID: {d['id']}  Name: {d['component']['name']}")
    else:
        print(f"❌ Failed: {r.text}")

"""
PARAMETER CONTEXT
"""
def list_parameter_contexts():
    r = requests.get(f"{NIFI_HOST}/flow/parameter-contexts", headers=HEADERS, verify=True)
    contexts = r.json().get("parameterContexts", [])
    return contexts

def parameter_context_exists(name):
    return any(c["component"]["name"] == name for c in list_parameter_contexts())

def create_parameter_context(
    name="Auditoria_Config",
    mysql_url="jdbc:mariadb://auditoria-db.cx...",
    mysql_user="your_username",
    mysql_password="your_password",
    snowflake_role="OPENFLOW_ADMIN",
    snowflake_warehouse="auditoria_warehouse",
    included_tables="auditoria.employee_test",
):
    if parameter_context_exists(name):
        print(f"\n⚠️  Parameter context '{name}' already exists — skipping.")
        return None
    body = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "description": "Parameter context for Auditoria ingestion pipeline",
            "parameters": [
                {"parameter": {"name": "Column_Filter_JSON",               "value": "[]",                           "sensitive": False}},
                {"parameter": {"name": "Concurrent_Snapshot_Queries",      "value": "2",                            "sensitive": False}},
                {"parameter": {"name": "Destination_Database",             "value": "auditoria",                    "sensitive": False}},
                {"parameter": {"name": "Included_Table_Names",             "value": included_tables,                "sensitive": False}},
                {"parameter": {"name": "Included_Table_Regex",             "value": "",                             "sensitive": False}},
                {"parameter": {"name": "Ingestion_Type",                   "value": "incremental",                  "sensitive": False}},
                {"parameter": {"name": "Merge_Task_Schedule_CRON",         "value": "0 */15 * * * ?",               "sensitive": False}},
                {"parameter": {"name": "MySQL_Connection_URL",             "value": mysql_url,                      "sensitive": False}},
                {"parameter": {"name": "MySQL_JDBC_Driver",                "value": "mariadb-java-client-3.5.7.jar","sensitive": False}},
                {"parameter": {"name": "MySQL_Password",                   "value": mysql_password,                 "sensitive": True}},
                {"parameter": {"name": "MySQL_Username",                   "value": mysql_user,                     "sensitive": False}},
                {"parameter": {"name": "Object_Identifier_Resolution",     "value": "CASE_SENSITIVE",               "sensitive": False}},
                {"parameter": {"name": "Re-read_Tables_in_State",          "value": "Any active",                   "sensitive": False}},
                {"parameter": {"name": "Snowflake_Account_Identifier",     "value": "",                             "sensitive": False}},
                {"parameter": {"name": "Snowflake_Authentication",         "value": "SNOWFLAKE_MANAGED",            "sensitive": False}},
                {"parameter": {"name": "Snowflake_Connection_String",      "value": "STANDARD",                     "sensitive": False}},
                {"parameter": {"name": "Snowflake_Private_Key",            "value": "",                             "sensitive": True}},
                {"parameter": {"name": "Snowflake_Private_Key_File",       "value": "",                             "sensitive": False}},
                {"parameter": {"name": "Snowflake_Private_Key_Passphrase", "value": "",                             "sensitive": True}},
                {"parameter": {"name": "Snowflake_Role",                   "value": snowflake_role,                 "sensitive": False}},
                {"parameter": {"name": "Snowflake_Username",               "value": "",                             "sensitive": False}},
                {"parameter": {"name": "Snowflake_Warehouse",              "value": snowflake_warehouse,            "sensitive": False}},
                {"parameter": {"name": "Starting_Binlog_Position",         "value": "latest",                       "sensitive": False}},
            ]
        }
    }
    r = requests.post(f"{NIFI_HOST}/parameter-contexts", headers=HEADERS, json=body, verify=True)
    print(f"\n[create-parameter-context] {r.status_code}")
    if r.ok:
        d = r.json()
        print(f"✅ Parameter Context Created — ID: {d['id']}  Name: {d['component']['name']}")
        return d
    else:
        print(f"❌ Failed: {r.text}")
        return None

"""
IMPORT FLOW FROM REGISTRY
"""
def get_registry_id_by_name(name):
    """Fetch the registry client ID by name — avoids hardcoding the ID."""
    clients = list_registry_clients()
    for c in clients:
        if c["component"]["name"] == name:
            return c["id"]
    print(f"⚠️  Registry client '{name}' not found.")
    return None

def import_flow_from_registry(
    flow_name="first-flow-import",
    bucket_id="default",
    flow_id="first-flow",
    version="3d707bb454785da759f70049520f8189cd3ca24a",
    branch="dev",
    position_x=0,
    position_y=0,
    registry_name=CLIENT_NAME,   # looks up ID automatically from name
):
    registry_id = get_registry_id_by_name(registry_name)
    if not registry_id:
        print("❌ Cannot import flow — registry client not found.")
        return None

    body = {
        "revision": {"version": 0},
        "component": {
            "name": flow_name,
            "position": {"x": position_x, "y": position_y},
            "versionControlInformation": {
                "registryId": registry_id,
                "bucketId": bucket_id,
                "flowId": flow_id,
                "version": version,
                "branch": branch,
            }
        }
    }

    r = requests.post(
        f"{NIFI_HOST}/process-groups/root/process-groups",
        headers=HEADERS,
        json=body,
        verify=True
    )
    print(f"\n[import-flow] {r.status_code}")

    if r.ok:
        d = r.json()
        print(f"✅ Flow Imported — ID: {d['id']}  Name: {d['component']['name']}")
        return d
    else:
        print(f"❌ Failed: {r.text}")
        return None

"""
ADDING PARAMETER CONTEXT TO PROCESS GROUP
"""
def get_parameter_context_id_by_name(name):
    for c in list_parameter_contexts():
        if c["component"]["name"] == name:
            return c["id"]
    print(f"⚠️  Parameter context '{name}' not found.")
    return None

def get_process_group_id_by_name(name, parent="root"):
    r = requests.get(
        f"{NIFI_HOST}/process-groups/{parent}/process-groups",
        headers=HEADERS, verify=True
    )
    for pg in r.json().get("processGroups", []):
        if pg["component"]["name"] == name:
            return pg["id"], pg["revision"]["version"]
    print(f"⚠️  Process group '{name}' not found.")
    return None, None

def assign_parameter_context_to_flow(flow_name="first-flow-import", context_name="Auditoria_Config"):
    pg_id, pg_version = get_process_group_id_by_name(flow_name)
    if not pg_id:
        print("❌ Cannot assign — process group not found.")
        return None

    ctx_id = get_parameter_context_id_by_name(context_name)
    if not ctx_id:
        print("❌ Cannot assign — parameter context not found.")
        return None

    body = {
        "revision": {"version": pg_version},
        "component": {
            "id": pg_id,
            "parameterContext": {
                "id": ctx_id,
                "component": {"id": ctx_id}
            }
        }
    }

    r = requests.put(
        f"{NIFI_HOST}/process-groups/{pg_id}",
        headers=HEADERS,
        json=body,
        verify=True
    )
    print(f"\n[assign-parameter-context] {r.status_code}")
    if r.ok:
        print(f"✅ Parameter context '{context_name}' assigned to '{flow_name}'")
        return r.json()
    else:
        print(f"❌ Failed: {r.text}")
        return None

"""
MAIN
"""
if __name__ == "__main__":
    create_github_registry()
    create_parameter_context()
    import_flow_from_registry()
    assign_parameter_context_to_flow(flow_name="da test")

    # imported = import_flow_from_registry()
    # if imported:
    #     actual_name = imported["component"]["name"]
    #     assign_parameter_context_to_flow(flow_name=actual_name)
