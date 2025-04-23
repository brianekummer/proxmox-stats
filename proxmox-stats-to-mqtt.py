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
MQTT_TOPIC_PREFIX = os.getenv("MQTT_TOPIC_PREFIX")
MQTT_DISCOVERY_TOPIC = os.getenv("MQTT_DISCOVERY_TOPIC")
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

    Assumes a single Proxmox node for simplicity of code
    """
    all_vms = []
    for vm in get_json(f"/nodes/{PROXMOX_NODE}/qemu"):
        vm["type"] = "qemu"
        all_vms.append(vm)

    for vm in get_json(f"/nodes/{PROXMOX_NODE}/lxc"):
        vm["type"] = "lxc"
        all_vms.append(vm)

    return all_vms


def get_nas_stats(storage_data):
    # TODO- I think this can be simplified
    for storage in storage_data:
        if storage.get("storage") == "nas-public":
            total_bytes = storage.get("total", 0)
            used_bytes = storage.get("used", 0)
            used_fraction = storage.get("used_fraction", 0)

            return [
                {
                    "device_id": "proxmox_nas",
                    "device_model": "NAS",
                    "friendly_name": "NAS",
                    "state_topic_prefix": f"{MQTT_TOPIC_PREFIX}/proxmox_nas",
                    "stats": {
                        "proxmox_nas_disk_size_tb": round(total_bytes / (1000 ** 4), 2),  # TB
                        "proxmox_nas_disk_used_gb": round(used_bytes / (1000 ** 3), 2),   # GB
                        "proxmox_nas_disk_used_percent": round(used_fraction * 100, 1)
                    }
                }
            ]
    return None


def build_friendly_name(vmid, name, vm_type):
    """
    Build a friendly name for the sensor based on the VM type and ID.
    """
    friendly_name = name or f"{vm_type} {vmid}"
    return f"{friendly_name.replace('_', ' ').replace('-', ' ').title()} {'VM' if vm_type == 'qemu' else 'LXC'}"


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
    stats["host"] = [
        {
            "device_id": "proxmox_host",
            "device_model": "Proxmox Host",
            "friendly_name": "Proxmox Host",
            "state_topic_prefix": f"{MQTT_TOPIC_PREFIX}/proxmox_host",
            "stats": {
                "proxmox_host_memory_used_percent": round(used_mem / total_mem * 100, 2),
                "proxmox_host_disk_used_percent": round(used_disk / total_disk * 100, 2),
                "proxmox_host_uptime_seconds": node_status.get("uptime", 0)
            }
        }
    ]


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
            "type": vm_type,
            "friendly_name": build_friendly_name(vmid, vm_status.get("name"), vm_type),
            "device_id": sensor_key_prefix,
            "device_model": "VM" if vm_type == "qemu" else "LXC",
            "state_topic_prefix": f"{MQTT_TOPIC_PREFIX}/proxmox_{vm_type}/{vmid}",
            "stats": {
                f"{sensor_key_prefix}_uptime_seconds": uptime,
                f"{sensor_key_prefix}_memory_used_percent": round(mem_used / mem_alloc * 100, 2) if mem_alloc else 0,
                f"{sensor_key_prefix}_disk_used_percent": round(disk_used / disk_alloc * 100, 2) if disk_alloc else 0,
                f"{sensor_key_prefix}_percent_of_host_memory": round(mem_used / total_host_mem_MB * 100, 2),
                f"{sensor_key_prefix}_cores": cores,
                f"{sensor_key_prefix}_memory_allocated_mb": int(mem_alloc),
                f"{sensor_key_prefix}_disk_allocated_gb": round(disk_alloc, 2)
            }
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

    for category, devices in stats.items():
        for device in devices:
            for sensor_key, value in device["stats"].items():
                topic = f"{device["state_topic_prefix"]}/{sensor_key}"
                #client.publish(topic, value, retain=True)
                print(f"Published {sensor_key} to {topic}: {value}")

    client.loop_stop()


def publish_discovery_message(client, sensor_key, name, state_topic, device_info, unit=None, device_class=None):
    """
    Publish a Home Assistant MQTT discovery message for a sensor.

    I like this function being dumb- it just takes the parameters and publishes the message
    """

    payload = {
        "name": name,
        "state_topic": state_topic,
        "unique_id": sensor_key,
        "object_id": sensor_key,
        "device": device_info,
        "unit_of_measurement": unit,
        "state_class": "measurement"
    }

    # Add device_class only if it's not None
    if device_class:
        payload["device_class"] = device_class

    topic = f"{MQTT_DISCOVERY_TOPIC}/{sensor_key}/config"
    print(f"Publishing discovery message for {name} ({sensor_key}) to {topic} ...")
    print(f"    {json.dumps(payload, indent=2)}")
    #client.publish(f"{topic}", json.dumps(payload), retain=True)
    print(f"Published discovery message to: {topic}")
    
    time.sleep(0.5)  # Add a delay of 0.5 seconds between messages


def publish_sensor_discovery_by_device(client, device_id, device_model, device_friendly_name, state_topic_prefix, device_stats):
    """
    Publish discovery messages for a set of sensors.
    """
    sensor_definitions = [
        # Common sensors
        {"sensor_key": "memory_used_percent", "friendly_name": f"Memory Used", "unit": "%", "device_class": None},
        {"sensor_key": "disk_used_percent", "friendly_name": f"Disk Used", "unit": "%", "device_class": None},
        {"sensor_key": "uptime_seconds", "friendly_name": f"Uptime", "unit": "s", "device_class": "duration"},

        # VM/LXC-only sensors
        {"sensor_key": "percent_of_host_memory", "friendly_name": f"% of Host Memory", "unit": "%", "device_class": None},
        {"sensor_key": "cores", "friendly_name": f"Cores", "unit": "", "device_class": None},
        {"sensor_key": "memory_allocated_mb", "friendly_name": f"Memory Allocated", "unit": "MB", "device_class": "data_size"},
        {"sensor_key": "disk_allocated_gb", "friendly_name": f"Disk Allocated", "unit": "GB", "device_class": "data_size"},
    
        # NAS-only sensors
        {"sensor_key": "disk_size_tb", "friendly_name": "Disk Size", "unit": "TB", "device_class": "data_size"},
        {"sensor_key": "disk_used_gb", "friendly_name": "Disk Used (GB)", "unit": "GB", "device_class": "data_size"},
    ]

    device_info = {
        "identifiers": [device_id],
        "manufacturer": "Proxmox",
        "model": device_model,
        "name": device_friendly_name,
    }
    
    # Loop through all the list of sensors and publish discovery messages for each one
    for sensor_key, value in device_stats.items():
        # Find the appropriate sensor definition
        sensor_definition = next((s for s in sensor_definitions if sensor_key.endswith(s["sensor_key"])), None)

        publish_discovery_message(
            client,
            sensor_key=sensor_key,
            name=sensor_definition["friendly_name"],
            state_topic=f"{state_topic_prefix}/{sensor_key}",
            device_info=device_info,
            unit=sensor_definition["unit"],
            device_class=sensor_definition["device_class"] if sensor_definition["device_class"] and sensor_definition["device_class"].lower() != "none" else None
        )







def publish_discovery_messages(stats):
    """
    Publish MQTT discovery messages for all VMs/CTs, host, and NAS to Home Assistant.
    """
    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.connect(MQTT_BROKER, MQTT_PORT, 60)

    for device_category, devices in stats.items():
        for device in devices:
            publish_sensor_discovery_by_device(client, device["device_id"], device["device_model"], device["friendly_name"], device["state_topic_prefix"], device["stats"])

    client.loop_stop()




# Main script execution
if __name__ == "__main__":
    args = parse_args()
    
    stats = collect_stats()
    publish_all_stats_to_mqtt(stats)

    if args.publish_discovery:
        publish_discovery_messages(stats)

    print("Stats published to MQTT.")

