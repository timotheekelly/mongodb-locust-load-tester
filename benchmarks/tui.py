"""Interactive terminal UI for the orchestrator, built with Textual.

This is a thin wrapper: it builds the same `python -m benchmarks.orchestrator
...` command line you'd type by hand, runs it as a subprocess, and streams
its output live -- no benchmarking logic lives here, so behavior always
matches the CLI exactly.

Run it the same way you'd run the orchestrator directly (same env
requirements -- `.env` sourced, `SSL_CERT_FILE` set):

    .venv/bin/python -m benchmarks.tui

Pick profiles/tiers with the checkboxes, adjust volume/run-time/users if
needed, hit Run. Output streams into the log pane below; Ctrl+C (or the
Cancel button) stops the run early.
"""

import asyncio
import shlex
import sys
from pathlib import Path

import yaml
from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Button, Checkbox, Footer, Header, Input, Label, RichLog

from benchmarks.orchestrator import ALL_PROFILES

TIERS_FILE = Path(__file__).resolve().parent.parent / "tiers.yaml"


def _load_tier_labels() -> list[str]:
    with open(TIERS_FILE) as f:
        data = yaml.safe_load(f)
    return [t["tier_label"] for t in data.get("tiers", [])]


class OrchestratorTUI(App):
    """Pick profiles/tiers/settings, run the orchestrator, watch it stream live."""

    CSS = """
    #controls {
        height: auto;
        border: solid $accent;
        padding: 1;
    }
    #checkboxes {
        height: auto;
    }
    .group {
        width: 1fr;
        height: auto;
        border: solid $panel;
        padding: 0 1;
    }
    #settings {
        height: auto;
    }
    #settings Input {
        width: 12;
        margin-right: 1;
    }
    #buttons {
        height: auto;
        margin-top: 1;
    }
    #log {
        border: solid $accent;
    }
    """

    BINDINGS = [("ctrl+c", "cancel_run", "Cancel run"), ("q", "quit", "Quit")]

    def __init__(self) -> None:
        super().__init__()
        self._tier_labels = _load_tier_labels()
        self._proc: asyncio.subprocess.Process | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="controls"):
            with Horizontal(id="checkboxes"):
                with Vertical(classes="group"):
                    yield Label("Profiles")
                    for profile in ALL_PROFILES:
                        yield Checkbox(profile, id=f"profile-{profile}")
                with Vertical(classes="group"):
                    yield Label("Tiers")
                    for tier in self._tier_labels:
                        yield Checkbox(tier, id=f"tier-{tier}")
            with Horizontal(id="settings"):
                yield Label("Volume:")
                yield Input(value="tiny", id="volume")
                yield Label("Users:")
                yield Input(placeholder="default", id="users")
                yield Label("Spawn rate:")
                yield Input(placeholder="default", id="spawn-rate")
                yield Label("Run time:")
                yield Input(placeholder="default", id="run-time")
            with Horizontal(id="buttons"):
                yield Button("Run", id="run", variant="success")
                yield Button("Cancel", id="cancel", variant="error", disabled=True)
        yield RichLog(id="log", wrap=True, highlight=True, markup=True)
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "run":
            self.run_orchestrator()
        elif event.button.id == "cancel":
            self.action_cancel_run()

    def _selected(self, prefix: str) -> list[str]:
        return [
            cb.label.plain
            for cb in self.query(Checkbox)
            if cb.id and cb.id.startswith(prefix) and cb.value
        ]

    def _build_command(self) -> list[str] | None:
        profiles = self._selected("profile-")
        tiers = self._selected("tier-")
        log = self.query_one("#log", RichLog)

        if not tiers:
            log.write("[red]Pick at least one tier before running.[/red]")
            return None

        cmd = [sys.executable, "-m", "benchmarks.orchestrator"]
        if profiles:
            cmd += ["--profile", ",".join(profiles)]
        cmd += ["--tier", ",".join(tiers)]
        cmd += ["--volume", self.query_one("#volume", Input).value or "tiny"]

        for flag, widget_id in [("--users", "users"), ("--spawn-rate", "spawn-rate"), ("--run-time", "run-time")]:
            value = self.query_one(f"#{widget_id}", Input).value.strip()
            if value:
                cmd += [flag, value]

        return cmd

    @property
    def _run_button(self) -> Button:
        return self.query_one("#run", Button)

    @property
    def _cancel_button(self) -> Button:
        return self.query_one("#cancel", Button)

    def run_orchestrator(self) -> None:
        if self._proc is not None:
            return
        cmd = self._build_command()
        if cmd is None:
            return
        log = self.query_one("#log", RichLog)
        log.clear()
        log.write(f"[bold]$ {shlex.join(cmd)}[/bold]")
        self._run_button.disabled = True
        self._cancel_button.disabled = False
        self._stream_command(cmd)

    @work(exclusive=True)
    async def _stream_command(self, cmd: list[str]) -> None:
        log = self.query_one("#log", RichLog)
        try:
            self._proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            assert self._proc.stdout is not None
            async for raw_line in self._proc.stdout:
                log.write(raw_line.decode(errors="replace").rstrip())
            returncode = await self._proc.wait()
            log.write(f"[bold]-- exited with code {returncode} --[/bold]")
        except FileNotFoundError as e:
            log.write(f"[red]Failed to start: {e}[/red]")
        finally:
            self._proc = None
            self._run_button.disabled = False
            self._cancel_button.disabled = True

    def action_cancel_run(self) -> None:
        if self._proc is not None:
            self._proc.terminate()
            self.query_one("#log", RichLog).write("[yellow]-- cancel requested --[/yellow]")


def main() -> None:
    OrchestratorTUI().run()


if __name__ == "__main__":
    main()
