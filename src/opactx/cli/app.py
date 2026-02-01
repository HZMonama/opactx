import typer
import rich_click  # noqa: F401
from .build import build
from .init import init
from .inspect import inspect
from .list_plugins import list_plugins
from .run_opa import run_opa
from .validate import validate
from opactx import __version__

app = typer.Typer(
    name="opactx",
    help="Contract-first policy context compiler for OPA",
    no_args_is_help=True,
)

@app.command("version")
def version() -> None:
    """Show the opactx version."""
    typer.echo(f"opactx v{__version__}")

app.command()(build)
app.command()(init)
app.command()(validate)
app.command("inspect")(inspect)
app.command("list-plugins")(list_plugins)
app.command("run-opa")(run_opa)
