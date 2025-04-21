import argparse
import json
import os
import requests
import time
import psutil
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
import os


# Load environment variables from the .env file
load_dotenv()


# ====== CONFIGURATION ======
PROXMOX_HOST = os.getenv("PROXMOX_HOST")
PROXMOX_NODE = os.getenv("PROXMOX_NODE")
API_USER = os.getenv("API_USER")
API_REALM = os.getenv("API_REALM")
API_TOKEN_ID = os.getenv("API_TOKEN_ID")
API_TOKEN_SECRET = os.getenv("API_TOKEN_SECRET")

MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
MQTT_TOPIC = os.getenv("MQTT_TOPIC")
# ====== END CONFIGURATION ======


# Disable SSL warnings for insecure requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# Set up requests session with token for Proxmox API
session = requests.Session()
session.verify = False
session.headers.update({
    "Authorization": f"PVEAPIToken={API_USER}@{API_REALM}!{API_TOKEN_ID}={API_TOKEN_SECRET}"
})


def parse_args():
    parser = argparse.ArgumentParser(description="Proxmox Stats to MQTT")
    parser.add_argument('--publish-discovery', action='store_true', help="Publish MQTT discovery messages")
    return parser.parse_args()


def get_json(path):
    url = f"https://{PROXMOX_HOST}:8006/api2/json{path}"
    response = session.get(url)
    response.raise_for_status()
    return response.json()["data"]


def get_all_vms():
    """
    Retrieves all VMs and containers from all Proxmox nodes,
    explicitly adding a 'type' field to each for consistency.
    """
    all_vms = []
    nodes = get_json("/nodes")
    for node in nodes:
        node_name = node["node"]

        # QEMU VMs
        qemu_vms = get_json(f"/nodes/{node_name}/qemu")
        for vm in qemu_vms:
            vm["type"] = "qemu"
            all_vms.append(vm)

        # LXC containers
        lxc_vms = get_json(f"/nodes/{node_name}/lxc")
        for vm in lxc_vms:
            vm["type"] = "lxc"
            all_vms.append(vm)

    return all_vms


def collect_stats():
    """
    Collect stats from the Proxmox API and structure them for MQTT publishing.
    """
    stats = {}

    # Get host node stats
    nodes = get_json("/nodes")
    if not nodes:
        raise RuntimeError("No nodes found in Proxmox")

    # TODO- replace this with PROXMOX_NODE (assumes single node)
    node = nodes[0]  # Assuming single-node Proxmox setup
    node_name = node["node"]
    node_status = get_json(f"/nodes/{node_name}/status")

    total_mem = node_status["memory"]["total"]
    used_mem = node_status["memory"]["used"]
    total_disk = node_status["rootfs"]["total"]
    used_disk = node_status["rootfs"]["used"]

    stats["host"] = {
        "name": node_name,
        "memory_used_percent": round(used_mem / total_mem * 100, 2),
        "disk_used_percent": round(used_disk / total_disk * 100, 2),
        "uptime_seconds": node_status.get("uptime", 0),
    }

    # Get VM/container stats
    stats["vms"] = []
    total_host_mem_MB = total_mem / 1024 / 1024

    vms = get_all_vms()
    for vm in vms:
        vmid = vm["vmid"]
        vm_type = vm["type"]

        # Detailed info required to get accurate metrics
        vm_status = get_json(f"/nodes/{node_name}/{vm_type}/{vmid}/status/current")
        #print(f"Debug: VM {vmid} raw info: {vm_status}")

        mem_alloc = vm_status.get("maxmem", 0) / 1024 / 1024
        mem_used = vm_status.get("mem", 0) / 1024 / 1024
        disk_alloc = vm_status.get("maxdisk", 0) / 1024 / 1024 / 1024
        disk_used = vm_status.get("disk", 0) / 1024 / 1024 / 1024
        uptime = vm_status.get("uptime", 0)
        cores = vm_status.get("cpus", 0)

        vm_data = {
            "vmid": str(vmid),
            "name": vm_status.get("name", f"{vm_type}-{vmid}"),
            "type": vm_type,
            "uptime_seconds": uptime,
            "memory_used_percent": round(mem_used / mem_alloc * 100, 2) if mem_alloc else 0,
            "disk_used_percent": round(disk_used / disk_alloc * 100, 2) if disk_alloc else 0,
            "percent_of_host_memory": round((vm_status.get("mem", 0) / 1024 / 1024) / total_host_mem_MB * 100, 2),
            "cores": cores,
            "memory_allocated_MB": int(mem_alloc),
            "disk_allocated_GB": round(disk_alloc, 2),
        }

        stats["vms"].append(vm_data)


    # Get NAS stats
    storage_data = get_json(f"/nodes/{PROXMOX_NODE}/storage")
    nas_data = get_nas_stats(storage_data)
    if nas_data:
        stats["nas"] = nas_data
    else:
        stats["nas"] = {
            "error": "nas not found"
        }

    return stats




def get_nas_stats(storage_data):
    for storage in storage_data:
        if storage.get("storage") == "nas-public":
            total_bytes = storage.get("total", 0)
            used_bytes = storage.get("used", 0)
            used_fraction = storage.get("used_fraction", 0)

            return {
                "size_tb": round(total_bytes / 1e12, 2),  # in TB
                "used_gb": round(used_bytes / 1e9, 2),    # in GB
                "used_percent": round(used_fraction * 100, 1)
            }
    return None





def publish_stat_to_mqtt(topic, payload):
    # THIS IS CURRENTLY UNUSED- NOT SURE WHY I'D NEED THIS
    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()
    client.publish(topic, json.dumps(payload), retain=True)
    client.loop_stop()


def publish_all_stats_to_mqtt(stats):
    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    # Publish host stats
    for key, value in stats["host"].items():
        topic = f"{MQTT_TOPIC}/host/{key}"
        client.publish(topic, value, retain=True)

    # Publish each VM/container's stats
    for vm in stats["vms"]:
        vmid = vm["vmid"]
        for key, value in vm.items():
            if key == "vmid":
                continue  # already used in topic
            topic = f"{MQTT_TOPIC}/vm/{vmid}/{key}"
            client.publish(topic, value, retain=True)

    # Publish NAS stats
    if "nas" in stats:
        for key, value in stats["nas"].items():
            topic = f"{MQTT_TOPIC}/nas/{key}"
            client.publish(topic, value, retain=True)

    client.loop_stop()






def make_discovery_payload(vmid, sensor_key, name, state_topic, unit="", device_class=None, friendly_name=None):
    payload = {
        "unique_id": f"vm{vmid}_{sensor_key}",
        "state_topic": state_topic,
        "unit_of_measurement": unit,
        "value_template": "{{ value }}",
        "device": {
            "identifiers": [str(vmid)],
            "manufacturer": "Proxmox",
            "model": "VM/CT",
            "name": name,
        },
        "name": friendly_name or f"{name} {sensor_key.replace('_', ' ').title()}"
    }

    if device_class and device_class.lower() != "none":
        payload["device_class"] = device_class

    return payload


def publish_discovery_message(client, vmid, sensor_key, name, state_topic):
    """
    Helper function to publish a single discovery message to Home Assistant.
    """
    stat_config = {
        "memory_used_percent": {"unit": "%", "device_class": None},
        "disk_used_percent": {"unit": "%", "device_class": None},
        "uptime_seconds": {"unit": "s", "device_class": "duration"},
        "percent_of_host_memory": {"unit": "%", "device_class": None},
        "cores": {"unit": None, "device_class": None},
        "memory_allocated_MB": {"unit": "MB", "device_class": None},
        "disk_allocated_GB": {"unit": "GB", "device_class": None},
    }

    # Generate the discovery payload using our helper function
    payload = make_discovery_payload(vmid, sensor_key, name, state_topic)
    
    topic_base = f"homeassistant/sensor/vm{vmid}_{sensor_key}"
    client.publish(f"{topic_base}/config", json.dumps(payload), retain=True)
    print(f"Published discovery message to: {topic_base}/config")



def publish_discovery_messages():
    """
    Publish MQTT discovery messages for all VMs/CTs and host to Home Assistant.
    """
    # === Publish VM/CT Discovery ===
    vms = get_all_vms()
    for vm in vms:
        vmid = vm["vmid"]
        name = vm["name"]

        sensor_definitions = [
            {
                "sensor_key": "memory_used_percent",
                "friendly_name": f"{name} Memory Used",
                "unit": "%",
                "device_class": "None"
            },
            {
                "sensor_key": "disk_used_percent",
                "friendly_name": f"{name} Disk Used",
                "unit": "%",
                "device_class": "None"
            },
            {
                "sensor_key": "uptime_seconds",
                "friendly_name": f"{name} Uptime",
                "unit": "s",
                "device_class": "duration"
            },
            {
                "sensor_key": "percent_of_host_memory",
                "friendly_name": f"{name} % of Host Memory",
                "unit": "%",
                "device_class": "None"
            },
            {
                "sensor_key": "cores",
                "friendly_name": f"{name} Cores",
                "unit": "",
                "device_class": "None"
            },
            {
                "sensor_key": "memory_allocated_MB",
                "friendly_name": f"{name} Memory Allocated",
                "unit": "MB",
                "device_class": "None"
            },
            {
                "sensor_key": "disk_allocated_GB",
                "friendly_name": f"{name} Disk Allocated",
                "unit": "GB",
                "device_class": "None"
            }
        ]

        for sensor in sensor_definitions:
            state_topic = f"{MQTT_TOPIC_PREFIX}/vm/{vmid}/{sensor['sensor_key']}"
            payload = make_discovery_payload(
                vmid=vmid,
                sensor_key=sensor["sensor_key"],
                name=name,
                state_topic=state_topic,
                unit=sensor["unit"],
                device_class=sensor["device_class"],
                friendly_name=sensor["friendly_name"]
            )

            topic_base = f"homeassistant/sensor/vm{vmid}_{sensor['sensor_key']}"
            client.publish(f"{topic_base}/config", json.dumps(payload), retain=True)
            print(f"Published discovery message to: {topic_base}/config")

    # === Publish Host Discovery ===
    host_name = "Proxmox Host"
    host_id = "host"

    host_sensor_definitions = [
        {
            "sensor_key": "memory_used_percent",
            "friendly_name": f"{host_name} Memory Used",
            "unit": "%",
            "device_class": "None"
        },
        {
            "sensor_key": "disk_used_percent",
            "friendly_name": f"{host_name} Disk Used",
            "unit": "%",
            "device_class": "None"
        },
        {
            "sensor_key": "uptime_seconds",
            "friendly_name": f"{host_name} Uptime",
            "unit": "s",
            "device_class": "duration"
        }
    ]

    for sensor in host_sensor_definitions:
        state_topic = f"{MQTT_TOPIC_PREFIX}/host/{sensor['sensor_key']}"
        payload = make_discovery_payload(
            vmid=host_id,
            sensor_key=sensor["sensor_key"],
            name=host_name,
            state_topic=state_topic,
            unit=sensor["unit"],
            device_class=sensor["device_class"],
            friendly_name=sensor["friendly_name"]
        )

        topic_base = f"homeassistant/sensor/host_{sensor['sensor_key']}"
        client.publish(f"{topic_base}/config", json.dumps(payload), retain=True)
        print(f"Published discovery message to: {topic_base}/config")


    # Publish NAS discovery
    publish_nas_discovery(client)






def publish_nas_discovery(client):
    device = {
        "identifiers": ["proxmox_nas"],
        "name": "Proxmox NAS (nas)",
        "manufacturer": "Proxmox",
        "model": "External NAS Storage"
    }

    sensors = [
        {
            "name": "NAS Size",
            "unique_id": "proxmox_nas_size_tb",
            "state_topic": f"{MQTT_TOPIC}/nas/size_tb",
            "unit_of_measurement": "TB",
            "device_class": "data_size",
            "value_template": "{{ value | float }}",
        },
        {
            "name": "NAS Used",
            "unique_id": "proxmox_nas_used_gb",
            "state_topic": f"{MQTT_TOPIC}/nas/used_gb",
            "unit_of_measurement": "GB",
            "device_class": "data_size",
            "value_template": "{{ value | float }}",
        },
        {
            "name": "NAS Used %",
            "unique_id": "proxmox_nas_used_percent",
            "state_topic": f"{MQTT_TOPIC}/nas/used_percent",
            "unit_of_measurement": "%",
            "device_class": "battery",  # closest fitting class, just to get a nice gauge
            "value_template": "{{ value | float }}",
        }
    ]

    for sensor in sensors:
        topic = f"homeassistant/sensor/{sensor['unique_id']}/config"
        payload = {
            "name": sensor["name"],
            "state_topic": sensor["state_topic"],
            "unique_id": sensor["unique_id"],
            "unit_of_measurement": sensor["unit_of_measurement"],
            "device_class": sensor["device_class"],
            "value_template": sensor["value_template"],
            "device": device
        }
        client.publish(topic, json.dumps(payload), retain=True)
 





# Main script execution
if __name__ == "__main__":
    args = parse_args()
    
    stats = collect_stats()
    print(stats)
    #publish_all_stats_to_mqtt(stats)

    if args.publish_discovery:
        publish_discovery_messages()

    print("Stats published to MQTT.")

