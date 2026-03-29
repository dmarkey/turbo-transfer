"""Thunderbolt/USB-C link detection using IPv6 link-local (no sudo needed)."""

from __future__ import annotations

import platform
import re
import subprocess
import sys

from rich.console import Console

console = Console()


def detect_thunderbolt_interface() -> str | None:
    """Find the Thunderbolt network interface name."""
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
                            return lines[j].strip().split(":", 1)[1].strip()
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


def get_link_local_ipv6(iface: str) -> str | None:
    """Get the IPv6 link-local address (fe80::) for an interface. No sudo needed."""
    system = platform.system()
    try:
        if system == "Darwin":
            output = subprocess.check_output(["ifconfig", iface], text=True)
            # Match: inet6 fe80::xxxx%enX prefixlen 64 scopeid 0xN
            for line in output.splitlines():
                line = line.strip()
                if line.startswith("inet6 fe80"):
                    addr = line.split()[1]
                    # Strip scope id suffix if present (e.g. %en5)
                    addr = addr.split("%")[0]
                    return addr
        else:
            output = subprocess.check_output(["ip", "-6", "addr", "show", iface], text=True)
            # Match: inet6 fe80::xxxx/64 scope link
            for line in output.splitlines():
                line = line.strip()
                if "fe80::" in line and "inet6" in line:
                    addr = line.split()[1].split("/")[0]
                    return addr
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    return None


def ensure_link() -> tuple[str, str, str]:
    """Auto-detect Thunderbolt link and return (interface, local_ip, scope).

    Uses IPv6 link-local — no sudo, no configuration needed.
    Returns (iface, ipv6_address, iface) where iface is used as the scope ID.
    Exits on failure.
    """
    iface = detect_thunderbolt_interface()

    if iface is None and platform.system() == "Linux":
        # Try loading the module (only part that might need sudo)
        subprocess.run(
            ["sudo", "modprobe", "thunderbolt-net"],
            capture_output=True,
        )
        iface = detect_thunderbolt_interface()

    if iface is None:
        console.print("[red]No Thunderbolt interface found. Is the cable connected?[/red]")
        sys.exit(1)

    local_ip = get_link_local_ipv6(iface)
    if local_ip is None:
        console.print(f"[red]No IPv6 link-local address on {iface}. Is the link up?[/red]")
        sys.exit(1)

    console.print(f"[green]Link ready: {iface} @ {local_ip}%{iface}[/green]")
    return iface, local_ip, iface


def discover_peer_ipv6(iface: str, timeout: float = 5.0) -> str | None:
    """Discover peer's IPv6 link-local address using multicast ping (no sudo).

    Pings ff02::1 (all-nodes multicast) and finds the other responder.
    """
    local_ip = get_link_local_ipv6(iface)
    system = platform.system()

    try:
        if system == "Darwin":
            # macOS: ping6 -c 2 -I <iface> ff02::1
            output = subprocess.check_output(
                ["ping6", "-c", "3", "-I", iface, "ff02::1"],
                text=True,
                timeout=timeout,
                stderr=subprocess.DEVNULL,
            )
        else:
            # Linux: ping -6 -c 2 -I <iface> ff02::1
            output = subprocess.check_output(
                ["ping", "-6", "-c", "3", "-I", iface, "ff02::1"],
                text=True,
                timeout=timeout,
                stderr=subprocess.DEVNULL,
            )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        return None

    # Parse responses — find fe80:: addresses that aren't ours
    peers = set()
    for line in output.splitlines():
        match = re.search(r"from\s+(fe80[:%]\S+)", line, re.IGNORECASE)
        if not match:
            match = re.search(r"from\s+(fe80[^\s,]+)", line, re.IGNORECASE)
        if match:
            addr = match.group(1)
            # Normalize: strip scope suffix for comparison
            bare = addr.split("%")[0]
            if bare != local_ip:
                peers.add(bare)

    if peers:
        return peers.pop()
    return None


def check_link() -> dict:
    """Check link status. Returns status dict."""
    iface = detect_thunderbolt_interface()

    status = {
        "interface": iface,
        "local_ipv6": None,
        "peer_ipv6": None,
        "link_up": False,
        "peer_reachable": False,
    }

    if iface is None:
        return status

    local_ip = get_link_local_ipv6(iface)
    status["local_ipv6"] = f"{local_ip}%{iface}" if local_ip else None
    status["link_up"] = local_ip is not None

    if status["link_up"]:
        peer = discover_peer_ipv6(iface, timeout=3.0)
        if peer:
            status["peer_ipv6"] = f"{peer}%{iface}"
            status["peer_reachable"] = True

    return status
