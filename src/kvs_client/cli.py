import click

@click.command()
def hello():
    click.echo("This is my first click command")