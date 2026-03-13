import nipyapi
import urllib3

urllib3.disable_warnings()

# Configure NiFi connection
nipyapi.config.nifi_config.host = "https://of--rbfuedq-wbc50273.snowflakecomputing.app/auditoriaruntime/nifi-api"
nipyapi.config.nifi_config.verify_ssl = False

cookie_value = """__Secure-Request-Token=293d183e-b8e2-4394-98af-2b5d239f778c; openflow-instance-id=a72597ff-c14e-4dde-b7de-e0e2bd14678d; __Secure-Gateway-Bearer-Token=eyJraWQiOiJlOWMxZDkyZi1kZDAyLTQ2ODktYjZhNS0xZmY1MmI4NjdkZmQiLCJhbGciOiJQUzUxMiJ9.eyJzdWIiOiJPUEVORkxPV19VU0VSIiwiYXVkIjoiL2F1ZGl0b3JpYXJ1bnRpbWUiLCJuYmYiOjE3NzMzODgwMDAsImlzcyI6Imh0dHBzOi8vb2YtLXJiZnVlZHEtd2JjNTAyNzMuc25vd2ZsYWtlY29tcHV0aW5nLmFwcC9hdWRpdG9yaWFydW50aW1lIiwiZ3JvdXBzIjpbIk1PTklUT1IiLCJPUEVSQVRFIiwiT1dORVJTSElQIiwiU25vd2ZsYWtlLUN1cnJlbnQtUm9sZS1PUEVORkxPV19BRE1JTiIsIlVTQUdFIl0sImV4cCI6MTc3MzM4ODYwMCwiaWF0IjoxNzczMzg4MDAwLCJqdGkiOiIzNTQ4ODY1Ni01MjAyLTRiNzAtYTZmNC01M2VhNjA1YzRmZDAifQ.Qbr408y-svPkBa9UdHpk5-hINGRYULXmbG4eFIVSfeTOFObYoFsiQTTcnkHJP0wgKr3dILYr9mjCxPWj20PkUiYvGGMHYLgkkqP0mEvxAxxNjTO4YmkgvF9z6lNQmp0LwRWoNr9Y6Ws04Z1m9_mWF-dS72sQaDfymnXTNcog-KD5YZVM-G-27BQloh1_IiISO4q7YNOGhDgWUkHZf8a0bxytZHi_oYb1TVvZVw_oYbOfVnpxEy87yi6Piv-F9dfKKGp5JtKF6_FHzhn43lsSwGIEoyQj_A2KeizgoLITrr7Ur82U_zY9OLk_-lZWg5QdiX9JY0lF0mnwLkaaKWLUk-AADWX9FqVECXpQPaKtPGIm3QVCmC6NSCmkHgxIo9ki6xHvRJwFW-2MRq4TeDZoUQmSsw5LEdsgtkuxbnprGY7-XkRHM2cw77f-SfVhf9pzGCRztUa5DdS1l8vnrH5DSoge1lAfL2mvT_BIeVSCrG3qZ0K7qeS-DPUbJq1wzLvkR9s13oaltXvAGO3JSaPew2PXGzboRMha19GzgJp7BDPuaD-G8fj4GS49OvEh193JYydAbrsGEDQwOKwhtl-o9bBRG0lfvKayUx7nVKVQmHPQLBgC9UbF7oR9ygf7LEErUBf5BQLD3TlEKIhro9T8DYKC9HkaPPScRn3baX7kTiA"""

# Monkey patch the HTTP methods to include the cookie
original_get = nipyapi.nifi.rest.RESTClientObject.GET

def patched_get(self, url, headers=None, query_params=None, _preload_content=True, _request_timeout=None):
    if headers is None:
        headers = {}
    headers['Cookie'] = cookie_value
    return original_get(self, url, headers, query_params, _preload_content, _request_timeout)

# Apply the patch for GET method only (needed for version check)
nipyapi.nifi.rest.RESTClientObject.GET = patched_get

try:
    # Get version info
    version_info = nipyapi.system.get_nifi_version_info()
    print("✅ Successfully connected to NiFi")
    print(f"   NiFi Version: {version_info.ni_fi_version}")
    
except Exception as e:
    print(f"❌ Error: {e}")
