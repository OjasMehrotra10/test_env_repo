import requests

"""
CONFIG
"""
NIFI_HOST   = "https://of--rbfuedq-wbc50273.snowflakecomputing.app/auditoriaruntime/nifi-api"
COOKIE      = """__Secure-Request-Token=a2f56fe7-ee98-4c57-8919-b7850c5e7b95; openflow-instance-id=a72597ff-c14e-4dde-b7de-e0e2bd14678d; __Secure-Gateway-Bearer-Token=eyJraWQiOiJlOWMxZDkyZi1kZDAyLTQ2ODktYjZhNS0xZmY1MmI4NjdkZmQiLCJhbGciOiJQUzUxMiJ9.eyJzdWIiOiJPUEVORkxPV19VU0VSIiwiYXVkIjoiL2F1ZGl0b3JpYXJ1bnRpbWUiLCJuYmYiOjE3NzMzOTg2MTQsImlzcyI6Imh0dHBzOi8vb2YtLXJiZnVlZHEtd2JjNTAyNzMuc25vd2ZsYWtlY29tcHV0aW5nLmFwcC9hdWRpdG9yaWFydW50aW1lIiwiZ3JvdXBzIjpbIk1PTklUT1IiLCJPUEVSQVRFIiwiT1dORVJTSElQIiwiU25vd2ZsYWtlLUN1cnJlbnQtUm9sZS1PUEVORkxPV19BRE1JTiIsIlVTQUdFIl0sImV4cCI6MTc3MzM5OTIxNCwiaWF0IjoxNzczMzk4NjE0LCJqdGkiOiJhYmY0MDI1NC1mZTUxLTRkNWMtYTlmYy1kZDI4MjNlZjQ4NGIifQ.dQeeeTlVTlK_RU8pFmoLT9x91DUuMaDn-PPbfDkL-IbwT-YKAbQKamX9YW-M2unFE3s7pUSg09Kx1NnwAhiddke2QNTinl6zBaBKxh1pK8dRchEe9bQKl-jtJsM6Ze7yXnTq0W76nUa1EO6siCCQInlM3tl-wavCMAgbN4Uklo_wNJN2QmgKv9miaPt6myQw5hd7rqMusG8_1qF0xTLhz0hOuSO6rNJbozuHd0OGl93d7TdGdy8due---A7mI_b4RgkaH5qkX37zxyj6b5OtT3_BrwQYFzybMWUj0_oVdw2Q3mISqdohqfKv8L5Qh1I5XkMfU24Alpdwc_QfazwnGW4pyEwugV68AfDdGij-OnKql_tKtYYbPzaovhSVfdkoQJWPTNkeIT7lldbA_QGSxo0Frq9sUW3Lxvsl0kKhfUCNIjmuXT3CDYuViXTOZQKu4x9yrLazqmQctqnSh8oKdlTSP_ENwJnWg5QuelnChbWLYe3ZRv2JLALXQkhDCJZLEv3XvWT6cFAIPxcwq5jxWYtf2kBsRPznQgfpVp4D0YxbKSGuqEYDdeXE0cC9PsVqpeXcToxzG-_ALoi-pEGDwgjomo7QzCrCnkwmP_qCJwKPgw2foamcOUJjkRxxRigi3OxuRzE9pt6v5L7-Uyid79QBM-tD567qQz7pGGvjGlQ; XSRF-TOKEN=9e252ea7-4501-4320-a885-f2db52c31a6e; JSESSIONID=CF8789B6855C4DEB341ABCB843CD2399"""
ORIGIN      = "https://of--rbfuedq-wbc50273.snowflakecomputing.app"
GITHUB_PAT  = """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"""
REPO_OWNER  = "OjasMehrotra10"
REPO_NAME   = "test_env_repo"
BRANCH      = "main"
CLIENT_NAME = "GitHub CI/CD Registry"

HEADERS = {
    "Content-Type": "application/json",
    "Cookie": COOKIE,
    "Origin": ORIGIN,
    "Referer": f"{ORIGIN}/auditoriaruntime/nifi/",
}

"""
REGISTRY HELPERS
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
PARAMETER CONTEXT HELPERS
"""
def list_parameter_contexts():
    r = requests.get(f"{NIFI_HOST}/flow/parameter-contexts", headers=HEADERS, verify=True)
    print(f"\n[parameter-contexts] {r.status_code}")
    contexts = r.json().get("parameterContexts", [])
    for ctx in contexts:
        c = ctx["component"]
        print(f"  {ctx['id']}  {c['name']}")
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
                {"parameter": {"name": "Column_Filter_JSON",               "value": "[]",                          "sensitive": False}},
                {"parameter": {"name": "Concurrent_Snapshot_Queries",      "value": "2",                           "sensitive": False}},
                {"parameter": {"name": "Destination_Database",             "value": "auditoria",                   "sensitive": False}},
                {"parameter": {"name": "Included_Table_Names",             "value": included_tables,               "sensitive": False}},
                {"parameter": {"name": "Included_Table_Regex",             "value": "",                            "sensitive": False}},
                {"parameter": {"name": "Ingestion_Type",                   "value": "incremental",                 "sensitive": False}},
                {"parameter": {"name": "Merge_Task_Schedule_CRON",         "value": "0 */15 * * * ?",              "sensitive": False}},
                {"parameter": {"name": "MySQL_Connection_URL",             "value": mysql_url,                     "sensitive": False}},
                {"parameter": {"name": "MySQL_JDBC_Driver",                "value": "mariadb-java-client-3.5.7.jar","sensitive": False}},
                {"parameter": {"name": "MySQL_Password",                   "value": mysql_password,                "sensitive": True}},
                {"parameter": {"name": "MySQL_Username",                   "value": mysql_user,                    "sensitive": False}},
                {"parameter": {"name": "Object_Identifier_Resolution",     "value": "CASE_SENSITIVE",              "sensitive": False}},
                {"parameter": {"name": "Re-read_Tables_in_State",          "value": "Any active",                  "sensitive": False}},
                {"parameter": {"name": "Snowflake_Account_Identifier",     "value": "",                            "sensitive": False}},
                {"parameter": {"name": "Snowflake_Authentication",         "value": "SNOWFLAKE_MANAGED",           "sensitive": False}},
                {"parameter": {"name": "Snowflake_Connection_String",      "value": "STANDARD",                    "sensitive": False}},
                {"parameter": {"name": "Snowflake_Private_Key",            "value": "",                            "sensitive": True}},
                {"parameter": {"name": "Snowflake_Private_Key_File",       "value": "",                            "sensitive": False}},
                {"parameter": {"name": "Snowflake_Private_Key_Passphrase", "value": "",                            "sensitive": True}},
                {"parameter": {"name": "Snowflake_Role",                   "value": snowflake_role,                "sensitive": False}},
                {"parameter": {"name": "Snowflake_Username",               "value": "",                            "sensitive": False}},
                {"parameter": {"name": "Snowflake_Warehouse",              "value": snowflake_warehouse,           "sensitive": False}},
                {"parameter": {"name": "Starting_Binlog_Position",         "value": "latest",                      "sensitive": False}},
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
MAIN
"""
if __name__ == "__main__":
    list_registry_types()
    create_github_registry()
    create_parameter_context()
