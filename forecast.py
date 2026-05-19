"""
forecast.py
===========
The master entry point for the entire weather forecasting pipeline.
Consolidates ingestion, execution, and cleanup into a single Typer CLI.
"""

import typer
import subprocess
import os
import shutil
import apprise
from rich.console import Console
from rich.panel import Panel
from typing import Optional

app = typer.Typer(
    help="Unified Weather Forecasting Pipeline Runner",
    add_completion=False,
    rich_markup_mode="rich"
)
console = Console()

def send_notification(title: str, body: str):
    """Sends a notification using the user's default apprise config."""
    try:
        apobj = apprise.Apprise()
        config = apprise.AppriseConfig()
        
        # Standard apprise config locations
        home_config = os.path.expanduser("~/.config/apprise")
        if os.path.exists(home_config):
            config.add(home_config)
            apobj.add(config)
            apobj.notify(title=title, body=body)
            console.print(f"[dim gray]Notification sent: {title}[/dim gray]")
    except Exception as e:
        console.print(f"[red]Warning: Failed to send notification: {e}[/red]")

@app.command()
def clean(
    local: bool = typer.Option(True, help="Remove the local 'tmp_ingest' folder"),
    cloud: bool = typer.Option(False, help="Trigger the 'gcp-cleanup-all.sh' script")
):
    """
    [bold red]Nukes[/bold red] temporary files and optionally cleans up cloud resources.
    """
    if local:
        with console.status("[bold red]Cleaning local temporary files..."):
            if os.path.exists("tmp_ingest"):
                shutil.rmtree("tmp_ingest")
                console.print("[green]✓[/green] Local cache (tmp_ingest) removed.")
            else:
                console.print("[yellow]![/yellow] No local cache found.")
    
    if cloud:
        console.print(Panel("[bold yellow]Triggering Global Cloud Cleanup (Async)...[/bold yellow]"))
        subprocess.run(["bash", "gcp-cleanup-all.sh"])
    
    send_notification("Cleanup Complete", f"Local: {local}, Cloud: {cloud}")

@app.command()
def ingest(
    date: str = typer.Argument(..., help="Target date in YYYY-MM-DD format"),
    time: str = typer.Option("12:00", help="Target time in HH:MM format"),
    bucket: str = typer.Option("overengineeredweather-run-data", help="GCS bucket name"),
    force: bool = typer.Option(False, "--force", "-f", help="Bypass local cache and force redownload")
):
    """
    [bold blue]Fetches[/bold blue] physical state from Copernicus and uploads to GCS.
    """
    if force:
        console.print("[yellow]Force flag detected. Clearing local cache...[/yellow]")
        if os.path.exists("tmp_ingest"):
            shutil.rmtree("tmp_ingest")
            os.makedirs("tmp_ingest")

    console.print(Panel(f"[bold blue]Starting Ingestion Pipeline[/bold blue]\nTarget: {date} at {time}\nBucket: {bucket}"))
    
    cmd = ["pipenv", "run", "python", "ingest_era5.py", "--date", date, "--time", time, "--bucket", bucket]
    result = subprocess.run(cmd)
    
    status = "Success" if result.returncode == 0 else "FAILED"
    send_notification(f"Ingestion {status}", f"Date: {date}\nTime: {time}\nBucket: {bucket}")

@app.command()
def run(
    date: str = typer.Argument(..., help="Target date for the forecast")
):
    """
    [bold magenta]Provisions[/bold magenta] a TPU and executes the GenCast ensemble.
    """
    console.print(Panel(f"[bold magenta]Launching Cloud TPU Pipeline[/bold magenta]\nForecast Date: {date}"))
    
    cmd = ["bash", "gcp-run-forecast.sh", "--date", date]
    result = subprocess.run(cmd)
    
    status = "Success" if result.returncode == 0 else "FAILED"
    send_notification(f"Forecast Run {status}", f"Date: {date}")

@app.command()
def test_notification(text: str = typer.Argument("This is a test notification", help="Custom text for the test notification")):
    """
    [bold cyan]Sends[/bold cyan] a test notification to verify apprise configuration.
    """
    send_notification("Test Notification", text)
    console.print("[green]✓[/green] Test notification sent.")

@app.callback()
def main():
    """
    [bold green]Kalshi Weather Arbitrage Pipeline[/bold green]
    
    Use this script to manage the end-to-end lifecycle of your forecasts.
    """
    pass

if __name__ == "__main__":
    app()
