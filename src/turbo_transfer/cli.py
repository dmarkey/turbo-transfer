"""CLI interface for Turbo Transfer."""

from __future__ import annotations

import asyncio
import os
import platform
import signal
import subprocess
import sys

import click
from rich.console import Console
from rich.table import Table

from .link import check_link, discover_peer_ipv6, ensure_link

console = Console()


@click.group()
@click.version_option()
def main():
    """Turbo Transfer — FUSE remote filesystem over Thunderbolt/USB-C."""
    pass


@main.command()
@click.argument("directory", default=".")
@click.option("--port", "-p", default=9876, help="Port to listen on")
def serve(directory: str, port: int):
    """Serve a directory to peers over Thunderbolt."""
    from .fileserver import start_server, TRANSFER_PORT

    iface, local_ip, scope_id = ensure_link()
    console.print(f"[dim]Thunderbolt link: {local_ip}%{iface}[/dim]")

    try:
        asyncio.run(start_server(directory))
    except KeyboardInterrupt:
        console.print("\n[dim]Stopped.[/dim]")


def _do_unmount(mountpoint: str):
    """Unmount a FUSE filesystem."""
    try:
        if platform.system() == "Darwin":
            subprocess.run(["umount", mountpoint], capture_output=True)
        else:
            subprocess.run(["fusermount", "-u", mountpoint], capture_output=True)
    except Exception:
        pass


@main.command()
@click.argument("mountpoint", default="./remote")
@click.option("--peer", "-p", default=None, help="Peer IPv6 address (auto-detected if omitted)")
def mount(mountpoint: str, peer: str | None):
    """Mount the remote peer's filesystem locally."""
    from .fuse_ops import mount_remote
    from .rpc import ConnectionPool

    iface, local_ip, scope_id = ensure_link()

    if peer is None:
        console.print("[dim]Discovering peer...[/dim]")
        peer = discover_peer_ipv6(iface)
        if peer is None:
            console.print("[red]No peer found. Is the other machine running 'turbo serve'?[/red]")
            raise SystemExit(1)

    console.print(f"[green]Connecting to {peer}%{iface}[/green]")
    pool = ConnectionPool(peer, scope_id)

    # Verify connection with a hello
    from .rpc import rpc_call
    from .protocol import FsOp
    try:
        info = rpc_call(pool, FsOp.HELLO, {})
        console.print(f"[green]Connected to [bold]{info.get('hostname', 'peer')}[/bold] — serving {info.get('root', '/')}[/green]")
    except Exception as e:
        console.print(f"[red]Connection failed: {e}[/red]")
        raise SystemExit(1)

    abs_mountpoint = os.path.abspath(mountpoint)
    console.print(f"[green]Mounting at [bold]{abs_mountpoint}[/bold][/green]")
    console.print("[dim]Press Ctrl+C to unmount[/dim]")

    def _unmount_on_signal(signum, frame):
        console.print("\n[dim]Unmounting...[/dim]")
        pool.close_all()
        _do_unmount(abs_mountpoint)

    signal.signal(signal.SIGINT, _unmount_on_signal)
    signal.signal(signal.SIGTERM, _unmount_on_signal)

    try:
        mount_remote(pool, abs_mountpoint, foreground=True)
    finally:
        pool.close_all()


@main.command()
@click.argument("mountpoint")
def unmount(mountpoint: str):
    """Unmount a turbo mount."""
    abs_mp = os.path.abspath(mountpoint)
    system = platform.system()
    try:
        if system == "Darwin":
            subprocess.run(["umount", abs_mp], check=True)
        else:
            subprocess.run(["fusermount", "-u", abs_mp], check=True)
        console.print(f"[green]Unmounted {abs_mp}[/green]")
    except subprocess.CalledProcessError as e:
        console.print(f"[red]Failed to unmount: {e}[/red]")


@main.command()
def status():
    """Show link status and peer info."""
    info = check_link()

    table = Table(title="Turbo Transfer Status")
    table.add_column("Property", style="bold")
    table.add_column("Value")

    table.add_row("Interface", info["interface"] or "[red]Not found[/red]")
    table.add_row("Transport", info.get("transport") or "[red]None[/red]")
    table.add_row("Local IPv6", info["local_ipv6"] or "[red]None[/red]")
    table.add_row(
        "Link",
        "[green]UP[/green]" if info["link_up"] else "[red]DOWN[/red]",
    )
    table.add_row(
        "Peer",
        f"[green]{info['peer_ipv6']}[/green]" if info["peer_reachable"] else "[red]Not found[/red]",
    )

    console.print(table)


if __name__ == "__main__":
    main()
