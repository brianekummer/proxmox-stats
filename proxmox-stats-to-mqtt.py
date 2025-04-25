import argparse
import json
import os
import requests
import time
import paramiko
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
import os
import math
from datetime import datetime, timezone


# NOTES
#   - Every time mqtt.Client() is called, it logs the following warning
#     "DeprecationWarning: Callback API version 1 is deprecated, update to latest version"
#     - I tried to fix it multiple ways. The docs here https://github.com/eclipse-paho/paho.mqtt.python/blob/master/docs/migrations.rst#change-between-version-1x-and-20
#       say that 
#          client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)    
#       should work, but it doesn't. I don't know why. I don't care enough to figure it out right now.



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


# This needs to change, but is acceptable for now
SSH_HOST = os.getenv("SSH_HOST")
SSH_USERNAME = os.getenv("SSH_USERNAME")
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH")
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


def bytes_to_mib(num_bytes):
    # MB is a base-10 unit, 1 MB equals 1,000,000 bytes
    # MiB is a base-2 unit. 1 MiB equals 1,048,576 bytes
    return num_bytes / (1024 * 1024)

def bytes_to_gib(num_bytes):
    # GB uses the decimal system, 1 GB equals 1,000,000,000 bytes
    # GiB uses the binary system, 1 GiB equals 1,073,741,824 bytes
    return num_bytes / (1024 * 1024 * 1024)

def bytes_to_gb(num_bytes):
    # GB uses the decimal system, 1 GB equals 1,000,000,000 bytes
    # GiB uses the binary system, 1 GiB equals 1,073,741,824 bytes
    return num_bytes / (1000 * 1000 * 1000)

def bytes_to_tb(num_bytes):
    # TB uses the decimal system, 1 TB equals 1,000,000,000,000 bytes
    # TiB uses the binary system, 1 TiB equals 1,099,511,627,776 bytes
    return num_bytes / (1000 * 1000 * 1000 * 1000)


def get_friendly_name(vmid, name, vm_type):
    """
    Get a friendly name for the sensor based on the VM type and ID
    """
    friendly_name = name or f"{vm_type} {vmid}"
    return f"{friendly_name.replace('_', ' ').replace('-', ' ').title()} {'VM' if vm_type == 'qemu' else 'LXC'}"


def get_vm_disk_usage(ssh_host, ssh_username):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Connect to HA VM
    ssh.connect(ssh_host, username=ssh_username, key_filename=SSH_KEY_PATH)

    # Run the df command, use awk to get the 5th column of the second line (used percentage)
    stdin, stdout, stderr = ssh.exec_command("df -h / | awk 'NR==2 { print $5 }'")

    output = stdout.read().decode()
    ssh.close()

    return output.strip().rstrip('%')


def get_host_stats(vms):
    # Get host node stats
    node_status = get_json(f"/nodes/{PROXMOX_NODE}/status")
    #print(json.dumps(node_status, indent=2))
    
    # Total host memory
    #   - This is how much of the host's memory that is currently available, excluding the memory
    #     being used by vms and containers right now
    #   - This is NOT the amount of physical memory installed on the host
    #   - An example: the host has 16gb of physical memory, and all the VMs/containers are currently
    #     using 3GB (they're allotted 11GB). This shows 13GB.
    total_host_mem = node_status["memory"]["total"]
    
    used_mem = node_status["memory"]["used"]
    vm_allocated_mem = sum(vm.get("maxmem", 0) for vm in vms)
    #print(f"Total Memory: {total_mem} bytes ({round(total_mem/1024/1024/1024)} GB), Used Memory: {used_mem} bytes ({round(used_mem/1024/1024/1024)} GB)")

    total_disk = node_status["rootfs"]["total"]
    used_disk = node_status["rootfs"]["used"]
    
    total_cpus = node_status["cpuinfo"]["cpus"]
    vm_allocated_cpus = sum(vm.get("cpus", 0) for vm in vms)
    unallocated_cpus = total_cpus - vm_allocated_cpus

    unallocated_mem = total_host_mem - vm_allocated_mem

    #print(f"VMs and containers have allotted memory: {vm_allocated_mem} bytes ({round(vm_allocated_mem / 1024 / 1024 / 1024, 2)} GB)")
    #print(f"Unallocated memory for host: {unallocated_mem} bytes ({round(unallocated_mem / 1024 / 1024 / 1024, 2)} GB)")

    uptime_seconds = node_status.get("uptime", 0)
    last_boot_time = int(time.time()) - int(uptime_seconds)
    last_boot_time_iso = datetime.fromtimestamp(last_boot_time, tz=timezone.utc).isoformat()

    return [
        {
            "device_id": "proxmox_host",
            "device_model": "Proxmox Host",
            "friendly_name": "Proxmox Host",

            # I need this in collect_stats(), so I'll return this here
            "total_host_mem": total_host_mem,

            "state_topic_prefix": f"{MQTT_TOPIC_PREFIX}/proxmox_host",
            "stats": {
                "proxmox_host_disk_size_gb": math.ceil(bytes_to_gib(total_disk)), 
                "proxmox_host_total_cpus": total_cpus,
                "proxmox_host_unallocated_cpus": unallocated_cpus,

                # TODO LATER- I'm so confused !!!!
                #
                # I want memory to be what's not allocated to VMs, not the total memory of the host
                # I have 16GB of memory, 11 GB is allotted to vms and containers, I want this to show 5 GB
                # But Proxmox says the total memory of the host is 13GB (apparently Proxmox, etc is using 3GB of memory),
                # so that means I have ~ 1.5GB - 2GB of memory for the Proxmox host if all the containers max out
                #
                # Or, is used_mem (which is 5GB) is that what I want? Preoxmox API documentation is not clear. This
                # looks like it could be true, but everything I see implies it is not
                #
                #"proxmox_host_memory_size_gb": 

                "proxmox_host_memory_used_percent": round(used_mem / total_host_mem * 100, 2),
                "proxmox_host_disk_used_percent": round(used_disk / total_disk * 100, 2),
                "proxmox_host_last_boot_time": last_boot_time_iso
            }
        }
    ]


def get_vm_stats(vmid, vm_type, total_host_mem):
    vm_status = get_json(f"/nodes/{PROXMOX_NODE}/{vm_type}/{vmid}/status/current")

    sensor_key_prefix = f"proxmox_{vm_type}_{vmid}"

    mem_alloc = vm_status.get("maxmem", 0)
    mem_used = vm_status.get("mem", 0)
    uptime_seconds = vm_status.get("uptime", 0)
    last_boot_time = int(time.time()) - int(uptime_seconds)
    last_boot_time_iso = datetime.fromtimestamp(last_boot_time, tz=timezone.utc).isoformat()
    cpus = vm_status.get("cpus", 0)

    # Yes, this conversion is weird, but 32GB can be 32-34GB depending on how you do the math,
    # and doing it this way yields 32GB- convert to GiB and then round up.
    disk_alloc_gb = math.ceil(bytes_to_gib(vm_status.get("maxdisk", 0)))



    # TODO- This is very hacky and hard-coded for my HA setup
    # One option would be to have the env var reference the vmid, like maybe have
    #    SSH_HOST_QEMU_100=xxxx and SSH_USERNAME_QEMU_100=xxxxx
    if vmid == 100:
        vm_disk_used_percent = get_vm_disk_usage(SSH_HOST, SSH_USERNAME)
    else:
        disk_used_gb = math.ceil(bytes_to_gib(vm_status.get("disk", 0)))
        vm_disk_used_percent = round(disk_used_gb / disk_alloc_gb * 100, 2) if disk_alloc_gb else 0



    return {
        "friendly_name": get_friendly_name(vmid, vm_status.get("name"), vm_type),
        "device_id": sensor_key_prefix,
        "device_model": "VM" if vm_type == "qemu" else "LXC",
        "state_topic_prefix": f"{MQTT_TOPIC_PREFIX}/proxmox_{vm_type}/{vmid}",
        "stats": {
            f"{sensor_key_prefix}_last_boot_time": last_boot_time_iso,
            f"{sensor_key_prefix}_memory_used_percent": round(mem_used / mem_alloc * 100, 2) if mem_alloc else 0,
            f"{sensor_key_prefix}_disk_used_percent": vm_disk_used_percent,
            f"{sensor_key_prefix}_percent_of_host_memory": round(mem_used / total_host_mem * 100, 2),
            f"{sensor_key_prefix}_cpus": cpus,
            f"{sensor_key_prefix}_memory_allocated_mb": int(bytes_to_mib(mem_alloc)),
            f"{sensor_key_prefix}_disk_allocated_gb": round(disk_alloc_gb, 2)
        }
    }


def get_nas_stats(storage_data, storage_name):
    storage = next((s for s in storage_data if s.get("storage") == storage_name), None)
    if storage:
        return [
            {
                "device_id": "proxmox_nas",
                "device_model": "NAS",
                "friendly_name": "NAS",
                "state_topic_prefix": f"{MQTT_TOPIC_PREFIX}/proxmox_nas",
                "stats": {
                    # Yes, using TB/GB (decimal/1000) instead of TiB/GiB (binary/1024) here is inconsistent, but it matches
                    # the Proxmox dashboard and the stated capacity of the NAS (6 TB)
                    "proxmox_nas_disk_size_tb": round(bytes_to_tb(storage.get("total", 0)), 2),
                    "proxmox_nas_disk_used_gb": round(bytes_to_gb(storage.get("used", 0)), 2),
                    "proxmox_nas_disk_used_percent": round(storage.get("used_fraction", 0) * 100, 1)
                }
            }
        ]
    return None


def collect_stats():
    """
    Collect stats from the Proxmox API and structure them for MQTT publishing
    """
    vms = get_all_vms()
    stats = {
        "host": get_host_stats(vms),
        "vms": [],
        "nas": get_nas_stats(get_json(f"/nodes/{PROXMOX_NODE}/storage"), "nas-public")
    }

    # Get host stats
    stats["host"] = get_host_stats(vms)

    # Get VM/container stats
    stats["vms"] = []
    for vm in vms:
        stats["vms"].append(get_vm_stats(vm["vmid"], vm["type"], stats["host"][0]["total_host_mem"]))

    # Get NAS stats
    stats["nas"] = get_nas_stats(get_json(f"/nodes/{PROXMOX_NODE}/storage"), "nas-public")

    return stats


def publish_all_stats_to_mqtt(stats):
    print(f"Publishing stats to MQTT at {datetime.now()}")
    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    for category, devices in stats.items():
        for device in devices:
            for sensor_key, value in device["stats"].items():
                topic = device['state_topic_prefix'] + '/' + sensor_key
                client.publish(topic, value, retain=True)
                print(f"Published {sensor_key} to {topic}: {value}")

    client.loop_stop()


def publish_discovery_message(client, sensor_key, name, state_topic, device_info, unit=None, device_class=None, icon=None):
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
    if icon:
        payload["icon"] = icon

    topic = f"{MQTT_DISCOVERY_TOPIC}/{sensor_key}/config"
    client.publish(f"{topic}", json.dumps(payload), retain=True)
    print(f"Published discovery message to: {topic}")

    # Home Assistant seems to require a short delay between messages to avoid overwhelming the broker
    time.sleep(0.5)


def publish_sensor_discovery_by_device(client, device_id, device_model, device_friendly_name, state_topic_prefix, device_stats):
    """
    Publish discovery messages for a set of sensors.
    """
    sensor_definitions = [
        # Common sensors
        {"sensor_key": "memory_used_percent", "friendly_name": f"Memory Used", "unit": "%", "device_class": None, "icon": "mdi:memory"},
        {"sensor_key": "disk_used_percent", "friendly_name": f"Disk Used", "unit": "%", "device_class": None, "icon": "mdi:harddisk"},
        {"sensor_key": "last_boot_time", "friendly_name": f"Last Boot Time", "unit": None, "device_class": "timestamp", "icon": "mdi:clock-outline"},

        # Host-only sensors
        {"sensor_key": "disk_size_gb", "friendly_name": "Disk Size", "unit": "GB", "device_class": "data_size", "icon": "mdi:harddisk"},
        {"sensor_key": "unallocated_cpus", "friendly_name": f"Unallocated CPUs", "unit": None, "device_class": None, "icon": "mdi:cpu-64-bit"},

        # VM/LXC-only sensors
        {"sensor_key": "percent_of_host_memory", "friendly_name": f"% of Host Memory", "unit": "%", "device_class": None, "icon": "mdi:memory"},
        {"sensor_key": "cpus", "friendly_name": f"CPUs", "unit": "", "device_class": None, "icon": "mdi:cpu-64-bit"},
        {"sensor_key": "memory_allocated_mb", "friendly_name": f"Memory Allocated", "unit": "MB", "device_class": "data_size", "icon": "mdi:memory"},
        {"sensor_key": "disk_allocated_gb", "friendly_name": f"Disk Allocated", "unit": "GB", "device_class": "data_size", "icon": "mdi:harddisk"},
    
        # NAS-only sensors
        {"sensor_key": "disk_size_tb", "friendly_name": "Disk Size", "unit": "TB", "device_class": "data_size", "icon": "mdi:harddisk"},
        {"sensor_key": "disk_used_gb", "friendly_name": "Disk Used", "unit": "GB", "device_class": "data_size", "icon": "mdi:harddisk"},
    ]

    device_info = {
        "identifiers": [device_id],
        "manufacturer": "Proxmox",
        "model": device_model,
        "name": device_friendly_name,
    }
    
    for sensor_key, value in device_stats.items():
        # Find the appropriate sensor definition for this statistic
        sensor_definition = next((s for s in sensor_definitions if sensor_key.endswith(s["sensor_key"])), None)

        publish_discovery_message(
            client,
            sensor_key=sensor_key,
            name=sensor_definition["friendly_name"],
            state_topic=f"{state_topic_prefix}/{sensor_key}",
            device_info=device_info,
            unit=sensor_definition["unit"],
            device_class=sensor_definition["device_class"] if sensor_definition["device_class"] and sensor_definition["device_class"].lower() != "none" else None,
            icon=sensor_definition["icon"]
        )


def publish_discovery_messages(stats):
    """
    Publish MQTT discovery messages to Home Assistant for the Proxmox host, NAS, and all VMs/containers
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



    # TODO- While in dev
    # for HA vm, this will replace proxmox_qemu_100_disk_used_percent
    #ha_disk_used_pct = get_vm_disk_usage(SSH_HOST, SSH_USERNAME)
    #print(f"HA Disk Used: {ha_disk_used_pct} %")



    stats = collect_stats()
    publish_all_stats_to_mqtt(stats)

    if args.publish_discovery:
        publish_discovery_messages(stats)

    print("Stats published to MQTT.")

