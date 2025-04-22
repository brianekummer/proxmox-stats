import argparse
import json
import os
import requests
import time
import psutil
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
import os
import math


# ====== CONFIGURATION ======
load_dotenv()
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

        for vm in get_json(f"/nodes/{node_name}/qemu"):
            vm["type"] = "qemu"
            all_vms.append(vm)

        for vm in get_json(f"/nodes/{node_name}/lxc"):
            vm["type"] = "lxc"
            all_vms.append(vm)

    return all_vms


def get_nas_stats(storage_data):
    for storage in storage_data:
        if storage.get("storage") == "nas-public":
            total_bytes = storage.get("total", 0)
            used_bytes = storage.get("used", 0)
            used_fraction = storage.get("used_fraction", 0)

            return {
                "nas_disk_size_tb": round(total_bytes / (1000 ** 4), 2),  # TB
                "nas_disk_used_gb": round(used_bytes / (1000 ** 3), 2),   # GB
                "nas_disk_used_percent": round(used_fraction * 100, 1)
            }
    return None


def collect_stats():
    """
    Collect stats from the Proxmox API and structure them for MQTT publishing.
    """
    stats = {}

    # Get host node stats
    node_status = get_json(f"/nodes/{PROXMOX_NODE}/status")

    total_mem = node_status["memory"]["total"]
    used_mem = node_status["memory"]["used"]
    total_disk = node_status["rootfs"]["total"]
    used_disk = node_status["rootfs"]["used"]

    stats["host"] = {
        "name": PROXMOX_NODE,
        "proxmox_host_memory_used_percent": round(used_mem / total_mem * 100, 2),
        "proxmox_host_disk_used_percent": round(used_disk / total_disk * 100, 2),
        "proxmox_host_uptime_seconds": node_status.get("uptime", 0),
    }

    # Get VM/container stats
    stats["vms"] = []
    total_host_mem_MB = total_mem / 1024 / 1024

    vms = get_all_vms()
    for vm in vms:
        vmid = vm["vmid"]
        vm_type = vm["type"]

        # Get detailed info required for accurate metrics
        vm_status = get_json(f"/nodes/{PROXMOX_NODE}/{vm_type}/{vmid}/status/current")

        mem_alloc = vm_status.get("maxmem", 0) / 1024 / 1024
        mem_used = vm_status.get("mem", 0) / 1024 / 1024
        uptime = vm_status.get("uptime", 0)
        cores = vm_status.get("cpus", 0)

        # Yes, this conversion is weird, but 32GB can be 33-34GB depending on how you do the math
        disk_alloc = math.ceil(vm_status.get("maxdisk", 0) / (1024 ** 3))  # Convert to GiB and round up
        disk_used = math.ceil(vm_status.get("disk", 0) / (1024 ** 3))  # Convert to GiB and round up

        sensor_key_prefix = f"proxmox_{vm_type}_{vmid}"
        vm_data = {
            "vmid": str(vmid),
            "name": vm_status.get("name", f"{vm_type}-{vmid}"),
            "type": vm_type,
            f"{sensor_key_prefix}_uptime_seconds": uptime,
            f"{sensor_key_prefix}_memory_used_percent": round(mem_used / mem_alloc * 100, 2) if mem_alloc else 0,
            f"{sensor_key_prefix}_disk_used_percent": round(disk_used / disk_alloc * 100, 2) if disk_alloc else 0,
            f"{sensor_key_prefix}_percent_of_host_memory": round(mem_used / total_host_mem_MB * 100, 2),
            f"{sensor_key_prefix}_cores": cores,
            f"{sensor_key_prefix}_memory_allocated_mb": int(mem_alloc),
            f"{sensor_key_prefix}_disk_allocated_gb": round(disk_alloc, 2),
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


def publish_all_stats_to_mqtt(stats):
    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    # Publish host stats
    for key, value in stats["host"].items():
        topic = f"{MQTT_TOPIC}/proxmox_host/{key}"
        #client.publish(topic, value, retain=True)
        #print(f"Published {key} to {topic}: {value}")

    # Publish each VM/container's stats
    for vm in stats["vms"]:
        vmid = vm["vmid"]
        for key, value in vm.items():
            if vmid != "100":
                continue
            if key in ["vmid"]:
                continue  # Do not want to publish these values

            topic = f"{MQTT_TOPIC}/proxmox_{vm["type"]}/{vmid}/{key}"
            # TODO- uncomment the below once I validate the nas stats
            #client.publish(topic, value, retain=True)
            print(f"Published {key} to {topic}: {value}")

    # Publish NAS stats
    if "nas" in stats:
        for key, value in stats["nas"].items():
            topic = f"{MQTT_TOPIC}/nas/{key}"
            # TODO- Is right, just needs enabled
            #client.publish(topic, value, retain=True)
            #print(f"Published {key} to {topic}: {value}")

    client.loop_stop()


def publish_discovery_message(client, sensor_key, name, state_topic, unique_id, object_id, device_info, unit=None, device_class=None):
    """
    Publish a Home Assistant MQTT discovery message for a sensor.
    """
    payload = {
        "name": name,
        "state_topic": state_topic,
        "unique_id": unique_id,
        "object_id": object_id,
        "device": device_info,
        "unit_of_measurement": unit,
        "state_class": "measurement"
    }

    # Add device_class only if it's not None
    if device_class:
        payload["device_class"] = device_class

    topic_base = f"homeassistant/sensor/{object_id}"
    print(f"Publishing discovery message for {name} ({sensor_key}) to {topic_base}/config ...")
    print(f"    {json.dumps(payload, indent=2)}")
    #client.publish(f"{topic_base}/config", json.dumps(payload), retain=True)
    print(f"Published discovery message to: {topic_base}/config")
    
    time.sleep(0.5)  # Add a delay of 0.5 seconds between messages


def publish_nas_discovery_messages(client):
    """
    Publish discovery messages for NAS-related sensors.
    """
    device_info = {
        "identifiers": ["nas_storage"],
        "name": "NAS",
        "manufacturer": "Proxmox",
        "model": "External NAS Storage"
    }

    nas_sensors = [
        {"sensor_key": "nas_disk_size_tb", "friendly_name": "Disk Size", "unit": "TB", "device_class": "data_size"},
        {"sensor_key": "nas_disk_used_gb", "friendly_name": "Disk Used (GB)", "unit": "GB", "device_class": "data_size"},
        {"sensor_key": "nas_disk_used_percent", "friendly_name": "Disk Used (%)", "unit": "%", "device_class": None}
    ]

    for sensor in nas_sensors:
        print(f"Processing discovery for sensor: {sensor['sensor_key']}")
        state_topic = f"{MQTT_TOPIC}/nas/{sensor['sensor_key']}"
        object_id = unique_id = f"{sensor['sensor_key']}"

        publish_discovery_message(
            client,
            sensor_key=sensor["sensor_key"],
            name=sensor["friendly_name"],
            state_topic=state_topic,
            unique_id=unique_id,
            object_id=object_id,
            device_info=device_info,
            unit=sensor["unit"],
            device_class=sensor["device_class"]
        )


def generate_sensor_definitions(name, is_host=False):
    """
    Generate sensor definitions for VMs/CTs or the host.
    """
    base_sensors = [
        {"sensor_key": "memory_used_percent", "friendly_name": f"Memory Used", "unit": "%", "device_class": None},
        {"sensor_key": "disk_used_percent", "friendly_name": f"Disk Used", "unit": "%", "device_class": None},
        {"sensor_key": "uptime_seconds", "friendly_name": f"Uptime", "unit": "s", "device_class": "duration"}
    ]

    if not is_host:
        additional_sensors = [
            {"sensor_key": "percent_of_host_memory", "friendly_name": f"% of Host Memory", "unit": "%", "device_class": None},
            {"sensor_key": "cores", "friendly_name": f"Cores", "unit": "", "device_class": None},
            {"sensor_key": "memory_allocated_mb", "friendly_name": f"Memory Allocated", "unit": "MB", "device_class": "data_size"},
            {"sensor_key": "disk_allocated_gb", "friendly_name": f"Disk Allocated", "unit": "GB", "device_class": "data_size"}
        ]
        base_sensors.extend(additional_sensors)

    return base_sensors


def publish_sensor_discovery(client, vmid, name, sensor_definitions, is_host=False):
    """
    Publish discovery messages for a set of sensors.
    """
    device_info = {
        "identifiers": [str(vmid)],
        "manufacturer": "Proxmox",
        "model": "VM/CT" if not is_host else "Host",
        "name": name,
    }

    for sensor in sensor_definitions:
        base_id = f"proxmox_{'host' if is_host else f'vm{vmid}'}"
        state_topic = f"{MQTT_TOPIC}/{base_id}/{base_id}_{sensor['sensor_key']}"
        object_id = unique_id = f"{base_id}_{sensor['sensor_key']}"

        publish_discovery_message(
            client,
            sensor_key=object_id,
            name=sensor["friendly_name"],
            state_topic=state_topic,
            unique_id=unique_id,
            object_id=object_id,
            device_info=device_info,
            unit=sensor["unit"],
            device_class=sensor["device_class"] if sensor["device_class"] and sensor["device_class"].lower() != "none" else None
        )


def publish_discovery_messages():
    """
    Publish MQTT discovery messages for all VMs/CTs, host, and NAS to Home Assistant.
    """
    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.connect(MQTT_BROKER, MQTT_PORT, 60)

    # === Publish Host Discovery ===
    host_name = "Proxmox Host"
    host_sensor_definitions = generate_sensor_definitions(host_name, is_host=True)
    # TODO- uncomment the below once I validate the nas stats
    #publish_sensor_discovery(client, "", host_name, host_sensor_definitions, is_host=True)

    # === Publish VM/CT Discovery ===
    vms = get_all_vms()
    for vm in vms:
        vmid = vm["vmid"]
        if vmid != 100:
            continue
        name = vm["name"].replace("_", " ").replace("-", " ").title()
        sensor_definitions = generate_sensor_definitions(name)
        # TODO- uncomment the below once I validate the nas stats
        publish_sensor_discovery(client, vmid, name, sensor_definitions)

    # === Publish NAS Discovery ===
    # TODO- Is right, just needs enabled
    #publish_nas_discovery_messages(client)




# Main script execution
if __name__ == "__main__":
    args = parse_args()
    
    stats = collect_stats()
    publish_all_stats_to_mqtt(stats)

    if args.publish_discovery:
        publish_discovery_messages()

    print("Stats published to MQTT.")

