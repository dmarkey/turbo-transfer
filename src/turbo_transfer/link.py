"""Link detection: Thunderbolt networking (Mac↔Mac) or USB gadget NCM (Linux involved).

Both transports produce a network interface with IPv6 link-local.
The rest of the stack (RPC, FUSE) is identical regardless of transport.
"""

from __future__ import annotations

import platform
import re
import subprocess
import sys
import time

from rich.console import Console

console = Console()

# USB gadget NCM configuration
GADGET_NAME = "turbo"
GADGET_CONFIGFS = f"/sys/kernel/config/usb_gadget/{GADGET_NAME}"


# -- Interface detection --


def detect_thunderbolt_interface() -> str | None:
    """Find an active Thunderbolt Bridge network interface (macOS↔macOS)."""
    system = platform.system()
    try:
        if system == "Darwin":
            output = subprocess.check_output(
                ["networksetup", "-listallhardwareports"], text=True
            )
            lines = output.splitlines()
            for i, line in enumerate(lines):
                if "Thunderbolt Bridge" in line:
                    for j in range(i + 1, min(i + 5, len(lines))):
                        if lines[j].strip().startswith("Device:"):
                            iface = lines[j].strip().split(":", 1)[1].strip()
                            # Only return if the bridge is active
                            status = subprocess.check_output(
                                ["ifconfig", iface], text=True
                            )
                            if "status: active" in status:
                                return iface
            return None
        elif system == "Linux":
            output = subprocess.check_output(["ip", "link"], text=True)
            for line in output.splitlines():
                if "thunderbolt" in line.lower():
                    parts = line.split(":")
                    if len(parts) >= 2:
                        return parts[1].strip()
            return None
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def detect_usb_ncm_interface() -> str | None:
    """Find a USB NCM/CDC network interface (for gadget or host side)."""
    system = platform.system()
    try:
        if system == "Darwin":
            # macOS creates an ECM/NCM interface when a USB gadget is connected
            # It shows up as a new en* interface with USB Ethernet type
            output = subprocess.check_output(
                ["networksetup", "-listallhardwareports"], text=True
            )
            lines = output.splitlines()
            for i, line in enumerate(lines):
                # macOS labels USB NCM gadgets as various names
                if any(x in line for x in ["USB 10/100/1000 LAN", "RNDIS", "CDC", "USB Ethernet", "USB 10/100 LAN"]):
                    for j in range(i + 1, min(i + 5, len(lines))):
                        if lines[j].strip().startswith("Device:"):
                            return lines[j].strip().split(":", 1)[1].strip()
            # Also check for any recently-appeared interfaces
            output = subprocess.check_output(["ifconfig", "-l"], text=True)
            for iface in output.split():
                if iface.startswith("en"):
                    try:
                        detail = subprocess.check_output(
                            ["ifconfig", iface], text=True
                        )
                        if "status: active" in detail and "inet6 fe80" in detail:
                            # Check if it's a USB interface via system_profiler would be slow
                            # Just try interfaces that are active with IPv6
                            pass
                    except subprocess.CalledProcessError:
                        pass
            return None
        elif system == "Linux":
            output = subprocess.check_output(["ip", "link"], text=True)
            # USB gadget NCM creates usb0 or similar
            for line in output.splitlines():
                for name in ["usb0", "usb1", "ncm0", "ecm0"]:
                    if name in line:
                        parts = line.split(":")
                        if len(parts) >= 2:
                            return parts[1].strip()
            return None
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


# -- USB Gadget setup (Linux only) --


def _find_udc() -> str | None:
    """Find the USB Device Controller name."""
    try:
        output = subprocess.check_output(["ls", "/sys/class/udc"], text=True).strip()
        if output:
            return output.split()[0]
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    return None


def setup_usb_gadget() -> str | None:
    """Configure USB gadget NCM on Linux. Returns interface name or None.

    Requires: libcomposite and usb_f_ncm kernel modules.
    Requires: sudo for configfs writes.
    """
    if platform.system() != "Linux":
        return None

    # Check if already set up
    existing = detect_usb_ncm_interface()
    if existing:
        return existing

    udc = _find_udc()
    if udc is None:
        console.print("[red]No USB Device Controller found. USB gadget not supported.[/red]")
        return None

    console.print("[dim]Setting up USB gadget NCM...[/dim]")

    # Load modules
    subprocess.run(["sudo", "modprobe", "libcomposite"], check=True)
    subprocess.run(["sudo", "modprobe", "usb_f_ncm"], check=True)

    # Configure gadget via shell script (configfs requires root)
    script = f"""
set -e
GADGET="{GADGET_CONFIGFS}"

# Clean up if exists
if [ -d "$GADGET" ]; then
    echo "" > "$GADGET/UDC" 2>/dev/null || true
    rm -f "$GADGET/configs/c.1/ncm.usb0" 2>/dev/null || true
    rmdir "$GADGET/configs/c.1/strings/0x409" 2>/dev/null || true
    rmdir "$GADGET/configs/c.1" 2>/dev/null || true
    rmdir "$GADGET/functions/ncm.usb0" 2>/dev/null || true
    rmdir "$GADGET/strings/0x409" 2>/dev/null || true
    rmdir "$GADGET" 2>/dev/null || true
fi

mkdir -p "$GADGET"
echo 0x1d6b > "$GADGET/idVendor"   # Linux Foundation
echo 0x0104 > "$GADGET/idProduct"  # Multifunction Composite Gadget
echo 0x0100 > "$GADGET/bcdDevice"
echo 0x0200 > "$GADGET/bcdUSB"

mkdir -p "$GADGET/strings/0x409"
echo "turbo-transfer" > "$GADGET/strings/0x409/manufacturer"
echo "Turbo Transfer NCM" > "$GADGET/strings/0x409/product"
echo "000000000001" > "$GADGET/strings/0x409/serialnumber"

mkdir -p "$GADGET/functions/ncm.usb0"
mkdir -p "$GADGET/configs/c.1/strings/0x409"
echo "NCM Network" > "$GADGET/configs/c.1/strings/0x409/configuration"

ln -sf "$GADGET/functions/ncm.usb0" "$GADGET/configs/c.1/"

echo "{udc}" > "$GADGET/UDC"
"""
    result = subprocess.run(
        ["sudo", "bash", "-c", script],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        console.print(f"[red]Gadget setup failed: {result.stderr.strip()}[/red]")
        return None

    # Bring interface up
    time.sleep(0.5)
    iface = detect_usb_ncm_interface()
    if iface:
        subprocess.run(["sudo", "ip", "link", "set", iface, "up"], check=False)
        console.print(f"[green]USB gadget NCM ready: {iface}[/green]")
    return iface


def teardown_usb_gadget():
    """Remove USB gadget configuration."""
    if platform.system() != "Linux":
        return
    script = f"""
GADGET="{GADGET_CONFIGFS}"
if [ -d "$GADGET" ]; then
    echo "" > "$GADGET/UDC" 2>/dev/null || true
    rm -f "$GADGET/configs/c.1/ncm.usb0" 2>/dev/null || true
    rmdir "$GADGET/configs/c.1/strings/0x409" 2>/dev/null || true
    rmdir "$GADGET/configs/c.1" 2>/dev/null || true
    rmdir "$GADGET/functions/ncm.usb0" 2>/dev/null || true
    rmdir "$GADGET/strings/0x409" 2>/dev/null || true
    rmdir "$GADGET" 2>/dev/null || true
fi
"""
    subprocess.run(["sudo", "bash", "-c", script], capture_output=True)


# -- IPv6 link-local (common to both transports) --


def get_link_local_ipv6(iface: str) -> str | None:
    """Get the IPv6 link-local address (fe80::) for an interface."""
    system = platform.system()
    try:
        if system == "Darwin":
            output = subprocess.check_output(["ifconfig", iface], text=True)
            for line in output.splitlines():
                line = line.strip()
                if line.startswith("inet6 fe80"):
                    addr = line.split()[1]
                    addr = addr.split("%")[0]
                    return addr
        else:
            output = subprocess.check_output(["ip", "-6", "addr", "show", iface], text=True)
            for line in output.splitlines():
                line = line.strip()
                if "fe80::" in line and "inet6" in line:
                    addr = line.split()[1].split("/")[0]
                    return addr
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    return None


def _wait_for_ipv6(iface: str, timeout: float = 5.0) -> str | None:
    """Wait for an IPv6 link-local address to appear on an interface."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        ip = get_link_local_ipv6(iface)
        if ip:
            return ip
        time.sleep(0.3)
    return None


# -- Main entry point --


def ensure_link() -> tuple[str, str, str]:
    """Auto-detect and set up the best available link.

    Tries in order:
    1. Thunderbolt networking (Mac↔Mac)
    2. USB gadget NCM (Linux involved)

    Returns (iface, ipv6_address, scope_id).
    """
    system = platform.system()

    # 1. Try Thunderbolt networking
    iface = detect_thunderbolt_interface()
    if iface is None and system == "Linux":
        subprocess.run(["sudo", "modprobe", "thunderbolt-net"], capture_output=True)
        iface = detect_thunderbolt_interface()

    if iface:
        local_ip = _wait_for_ipv6(iface)
        if local_ip:
            console.print(f"[green]Thunderbolt link: {iface} @ {local_ip}%{iface}[/green]")
            return iface, local_ip, iface

    # 2. Try USB NCM (existing or set up new gadget)
    iface = detect_usb_ncm_interface()
    if iface is None and system == "Linux":
        iface = setup_usb_gadget()

    if iface:
        local_ip = _wait_for_ipv6(iface)
        if local_ip:
            console.print(f"[green]USB NCM link: {iface} @ {local_ip}%{iface}[/green]")
            return iface, local_ip, iface

    # 3. On macOS host side, wait a moment for the USB NCM interface to appear
    if system == "Darwin":
        console.print("[dim]Waiting for USB device to appear...[/dim]")
        for _ in range(10):
            iface = detect_usb_ncm_interface()
            if iface:
                local_ip = _wait_for_ipv6(iface)
                if local_ip:
                    console.print(f"[green]USB NCM link: {iface} @ {local_ip}%{iface}[/green]")
                    return iface, local_ip, iface
            time.sleep(1)

    console.print("[red]No link found. Check cable and ensure peer is running 'turbo-transfer serve'.[/red]")
    sys.exit(1)


def discover_peer_ipv6(iface: str, timeout: float = 5.0) -> str | None:
    """Discover peer's IPv6 link-local address using multicast ping."""
    local_ip = get_link_local_ipv6(iface)
    system = platform.system()

    try:
        if system == "Darwin":
            output = subprocess.check_output(
                ["ping6", "-c", "3", "-I", iface, "ff02::1"],
                text=True, timeout=timeout, stderr=subprocess.DEVNULL,
            )
        else:
            output = subprocess.check_output(
                ["ping", "-6", "-c", "3", "-I", iface, "ff02::1"],
                text=True, timeout=timeout, stderr=subprocess.DEVNULL,
            )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        return None

    peers = set()
    for line in output.splitlines():
        match = re.search(r"from\s+(fe80[:%]\S+)", line, re.IGNORECASE)
        if not match:
            match = re.search(r"from\s+(fe80[^\s,]+)", line, re.IGNORECASE)
        if match:
            addr = match.group(1)
            bare = addr.split("%")[0]
            if bare != local_ip:
                peers.add(bare)

    if peers:
        return peers.pop()
    return None


def check_link() -> dict:
    """Check link status."""
    status = {
        "interface": None,
        "transport": None,
        "local_ipv6": None,
        "peer_ipv6": None,
        "link_up": False,
        "peer_reachable": False,
    }

    # Check Thunderbolt first
    iface = detect_thunderbolt_interface()
    if iface:
        status["transport"] = "Thunderbolt"
    else:
        iface = detect_usb_ncm_interface()
        if iface:
            status["transport"] = "USB NCM"

    if iface is None:
        return status

    status["interface"] = iface
    local_ip = get_link_local_ipv6(iface)
    status["local_ipv6"] = f"{local_ip}%{iface}" if local_ip else None
    status["link_up"] = local_ip is not None

    if status["link_up"]:
        peer = discover_peer_ipv6(iface, timeout=3.0)
        if peer:
            status["peer_ipv6"] = f"{peer}%{iface}"
            status["peer_reachable"] = True

    return status
