import requests
import nipyapi

"""
========================
CONFIG
========================
"""
NIFI_HOST = "https://of--rbfuedq-wbc50273.snowflakecomputing.app/auditoriaruntime/nifi-api"

COOKIE = """"""
ORIGIN = "https://of--rbfuedq-wbc50273.snowflakecomputing.app"

GITHUB_PAT = ""
REPO_OWNER = "OjasMehrotra10"
REPO_NAME = "test_env_repo"
BRANCH = "main"
CLIENT_NAME = "GitHubFlowRegistryClient"

HEADERS = {
    "Content-Type": "application/json",
    "Cookie": COOKIE,
    "Origin": ORIGIN,
    "Referer": f"{ORIGIN}/auditoriaruntime/nifi/",
}

"""
========================
SETUP NIPYAPI
========================
"""
def setup_nipyapi():
    import nipyapi
    from nipyapi.nifi import ApiClient

    # 🔥 Create ApiClient using YOUR constructor
    api_client = ApiClient(
        host=NIFI_HOST,
        cookie=COOKIE   # <-- IMPORTANT (your class supports this!)
    )

    # Add extra headers (optional but recommended)
    api_client.default_headers.update({
        "Origin": ORIGIN,
        "Referer": f"{ORIGIN}/auditoriaruntime/nifi/",
        "User-Agent": "Mozilla/5.0",
    })

    # 🔥 Attach to nipyapi
    nipyapi.config.nifi_config.api_client = api_client
"""
========================
REGISTRY (requests)
========================
"""
def list_registry_clients():
    r = requests.get(f"{NIFI_HOST}/controller/registry-clients", headers=HEADERS)
    return r.json().get("registries", [])

def get_registry_id_by_name(name):
    for c in list_registry_clients():
        if c["component"]["name"] == name:
            return c["id"]
    return None

def create_or_update_github_registry():
    existing_id = get_registry_id_by_name(CLIENT_NAME)

    body = {
        "revision": {"version": 0},
        "component": {
            "name": CLIENT_NAME,
            "type": "org.apache.nifi.github.GitHubFlowRegistryClient",
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

    if existing_id:
        print("🔄 Updating registry PAT...")
        body["component"]["id"] = existing_id

        # Need version
        for c in list_registry_clients():
            if c["id"] == existing_id:
                body["revision"]["version"] = c["revision"]["version"]

        r = requests.put(
            f"{NIFI_HOST}/controller/registry-clients/{existing_id}",
            headers=HEADERS,
            json=body
        )
    else:
        print("➕ Creating registry...")
        r = requests.post(
            f"{NIFI_HOST}/controller/registry-clients",
            headers=HEADERS,
            json=body
        )

    print("Registry:", r.status_code)
    return r.json()

"""
========================
PARAMETER CONTEXT (nipyapi)
========================
"""
def create_parameter_context():
    contexts = nipyapi.nifi.FlowApi().get_parameter_contexts().parameter_contexts

    for c in contexts:
        if c.component.name == "Auditoria_Config":
            print("⚠️ Parameter context exists")
            return c

    print("➕ Creating parameter context...")

    body = nipyapi.nifi.ParameterContextEntity(
        revision=nipyapi.nifi.RevisionDTO(version=0),
        component=nipyapi.nifi.ParameterContextDTO(
            name="Auditoria_Config",
            parameters=[
                nipyapi.nifi.ParameterEntity(
                    parameter=nipyapi.nifi.ParameterDTO(
                        name="Snowflake_Role",
                        value="OPENFLOW_ADMIN"
                    )
                )
            ]
        )
    )

    return nipyapi.nifi.ParameterContextsApi().create_parameter_context(body)

"""
========================
IMPORT FLOW (nipyapi)
========================
"""
def import_flow():
    registry_id = get_registry_id_by_name(CLIENT_NAME)

    if not registry_id:
        print("❌ Registry not found")
        return None

    print("🚀 Importing flow via nipyapi...")

    return nipyapi.nifi.ProcessGroupsApi().create_process_group(
        id="root",
        body=nipyapi.nifi.ProcessGroupEntity(
            revision=nipyapi.nifi.RevisionDTO(version=0),
            component=nipyapi.nifi.ProcessGroupDTO(
                name="nipyapi-imported-flow",
                position=nipyapi.nifi.PositionDTO(x=0.0, y=0.0),
                version_control_information=nipyapi.nifi.VersionControlInformationDTO(
                    registry_id=registry_id,
                    bucket_id="default",
                    flow_id="first-flow",
                    version="3d707bb454785da759f70049520f8189cd3ca24a",
                    branch="dev",
                ),
            )
        )
    )

"""
========================
ASSIGN PARAM CONTEXT
========================
"""
"""
ASSIGN PARAM CONTEXT - FIXED VERSION
"""
"""
ASSIGN PARAM CONTEXT - FIXED VERSION
"""
def assign_parameter_context(imported_pg):
    target = imported_pg

    contexts = nipyapi.nifi.FlowApi().get_parameter_contexts().parameter_contexts

    ctx = None
    for c in contexts:
        if c.component.name == "Auditoria_Config":
            ctx = c

    if not ctx:
        print("❌ Context not found")
        return

    print("🔗 Assigning parameter context...")

    # Create a new process group entity with the parameter context reference
    update_entity = nipyapi.nifi.ProcessGroupEntity(
        revision=target.revision,
        component=nipyapi.nifi.ProcessGroupDTO(
            id=target.component.id,
            name=target.component.name,
            parameter_context=nipyapi.nifi.ParameterContextReferenceDTO(
                id=ctx.id
            )
        )
    )

    # Perform the update
    result = nipyapi.nifi.ProcessGroupsApi().update_process_group(
        id=target.id,
        body=update_entity
    )
    
    print("✅ Parameter context assigned successfully")
    return result
"""
========================
MAIN
========================
"""
if __name__ == "__main__":
    setup_nipyapi()

    create_or_update_github_registry()
    create_parameter_context()

    imported = import_flow()

    if imported:
        assign_parameter_context(imported)
