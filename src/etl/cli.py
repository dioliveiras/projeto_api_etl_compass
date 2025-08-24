import typer
from rich.console import Console

console = Console()

def main(
    start: str = typer.Option(None, help="YYYY-MM-DD"),
    end: str = typer.Option(None, help="YYYY-MM-DD"),
    out: str = typer.Option(..., help="Diretório de saída")
):
    console.rule("[bold blue]ETL START")
    console.print(f"Período: {start} → {end}")
    console.print(f"Saída: {out}")
    console.rule("[bold green]ETL END (ainda esqueleto)")

if __name__ == "__main__":
    typer.run(main)