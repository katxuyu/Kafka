import requests
import csv
import argparse

# Set up command-line argument parsing
parser = argparse.ArgumentParser(description='Fetch device data from TTN and create a CSV file.')
parser.add_argument('ttn_application_id', type=str, help='TTN Application ID to fetch devices for')
args = parser.parse_args()

# Base domains
app_domain_list = 'https://packetworx.eu1.cloud.thethings.industries'
app_domain_details = 'https://packetworx.au1.cloud.thethings.industries'

auth = 'NNSXS.EHYOKV7CO2543DSQFU4ATPMTBAFKIFICSKZ4ZEQ.C3WOHJHQ7NHRD2P2IGZ5QERYUJPXZCZCXT3WFLNKKQYNGRLZIOCA'  # Reminder to securely handle your auth token

# Use the command-line argument for the TTN application ID
ttn_application_id = args.ttn_application_id

# Function to perform HTTP GET requests
def ttn_http_get(url, headers=None, params=None):
    response = requests.get(url, headers=headers, params=params)
    return response.json() if response.ok else None, response.status_code

# Endpoint for listing devices under an application
ttn_devices_list_endpoint = f"{app_domain_list}/api/v3/applications/{ttn_application_id}/devices"
# Fetch the list of devices
ttn_command_1_output, ttn_command_1_status = ttn_http_get(ttn_devices_list_endpoint, headers={"Authorization": f"Bearer {auth}"})

csv_data = []
for device_info in ttn_command_1_output.get("end_devices", []):
    ids_info = device_info.get("ids", {})
    device_id = ids_info.get("device_id", "")

    if not device_id:
        continue

    # Endpoint for fetching detailed device information from a different domain
    ttn_command_2_endpoint = f"{app_domain_details}/api/v3/js/applications/{ttn_application_id}/devices/{device_id}?field_mask=root_keys"
    # Fetch detailed device information
    ttn_command_2_output, ttn_command_2_status = ttn_http_get(
        ttn_command_2_endpoint, 
        headers={"Authorization": f"Bearer {auth}", "Content-Type": "application/json"}
    )

    if ttn_command_2_output:
        app_key = ttn_command_2_output.get("root_keys", {}).get("app_key", {}).get("key", "")
    else:
        app_key = ""  # Default to empty string if data is not found or an error occurs

    # Constructing row for CSV
    row = {
        "create_action": "",
        "deveui": ids_info.get("dev_eui", ""),
        "devaddr": "",
        "device_profile": "",
        "joineui": ids_info.get("join_eui", ""),
        "appkey": app_key,
        "Connectivity Plans by Actility": "",
        "Routing Profile ID": "",
        "device name": device_id,
        "lat": "",
        "lon": "",
        "additional_info": f'created at: {device_info.get("created_at", "")}',
    }

    csv_data.append(row)

# Writing to CSV
csv_filename = "Actility Format.csv"
csv_columns = csv_data[0].keys()

with open(csv_filename, "w", newline="") as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=csv_columns)
    writer.writeheader()
    for row in csv_data:
        writer.writerow(row)

print(f"CSV file '{csv_filename}' created successfully.")
