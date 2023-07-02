from python_loaders.base import BasePythonLoader
import click
import os
from typing import List, Dict, Type
import glob
import os.path as op
from importlib import import_module
import inspect
from abc import ABC

os.environ['NUMEXPR_MAX_THREADS'] = '8'

def get_python_loaders_list() -> List[Type[BasePythonLoader]]:
    """
    Separating this logic from get_pythonloader_class, so we do a few assertions
    against the list of python_loaders in our tests (and prevent disasters).
    :return: List of pythonloaders Classes in the python_loaders folder.
    """

    # commenting out the autofinder to keep it for reference.
    # no idea why this does not work in production but work in local.

    base_path = op.dirname(op.abspath(__file__))
    python_loaders_path = op.join(
        base_path,
        'python_loaders',
        '**',
        '*.py'
    )

    pythonloaders_classes = []

    for name in glob.iglob(python_loaders_path, recursive=True):
        if name.endswith('__init__.py'):
            continue

        # could possibly be done in a better way,
        # but whatever works, right !?
        module_name = (
            name
            .replace(base_path + '/', '', 1)
            .split('.py')[0].replace("/", ".")
        )
        mod = import_module(module_name)

        for obj in dir(mod):
            potential_pythonloader = getattr(mod, obj)
            if not inspect.isclass(potential_pythonloader):
                continue
            if potential_pythonloader in (BasePythonLoader, ABC):
                continue
            if issubclass(potential_pythonloader, BasePythonLoader):
                pythonloaders_classes.append(potential_pythonloader)

    return pythonloaders_classes


def get_pythonloader_class(name: str) -> Type[BasePythonLoader]:
    """
    Function to return the pythonloader class (not the instance) for a given
    pythonloader name. The name must match the name attribute of an
    PythonLoader class.
    :return: PythonLoader class
    """
    for cls in get_python_loaders_list():
        if cls.name == name:
            return cls

    raise Exception(
        f"The pythonloader name '{name}' is not recognized.\n"
        "Check that the name matches the name attribute of expected pythonloader "
        "class"
    )


def cli_variables_parser(cli_variables: List = None) -> Dict:
    variables = dict()

    if not cli_variables:
        return variables

    for var in cli_variables:
        key, value = var.split("=")
        variables[key] = value
    return variables


pythonloader_logo = r"""

                  _   _                 _                 _               
                 | | | |               | |               | |              
      _ __  _   _| |_| |__   ___  _ __ | | ___   __ _  __| | ___ _ __ ___ 
     | '_ \| | | | __| '_ \ / _ \| '_ \| |/ _ \ / _` |/ _` |/ _ \ '__/ __|
     | |_) | |_| | |_| | | | (_) | | | | | (_) | (_| | (_| |  __/ |  \__ \
     | .__/ \__, |\__|_| |_|\___/|_| |_|_|\___/ \__,_|\__,_|\___|_|  |___/
     | |     __/ |                                                        
     |_|    |___/                                                                                                 

"""

success = """
                              
     ___       ___  ___  ___  ___  ___ 
    |___ |   )|    |    |___)|___ |___ 
     __/ |__/ |__  |__  |__   __/  __/
       
                                   
"""


@click.group()
def pythonloader():
    ...


@pythonloader.command()
@click.option("-n", "--name")
@click.option("-f", "--from", "date_from", default=None, required=False)
@click.option("-t", "--to", "date_to", default=None, required=False)
@click.option("-v", "--tenant", "tenant", default=None, required=False)
@click.argument('nargs', nargs=-1)
def run_api(name, date_from, date_to, tenant, nargs):
    click.echo(pythonloader_logo)
    click.secho(f"PYTHON_LOADER: {name.title()}\n", bold=True)
    click.echo(f"Start Date: {date_from}")
    click.echo(f"End Date: {date_to}")
    click.echo(f"Tenant: {tenant}\n\n")

    kw_variables = cli_variables_parser(nargs)

    if kw_variables:
        click.secho("Extra arguments:\n", bold=True)
        for key, value in kw_variables.items():
            click.echo(f"{key} => {value}")
        click.echo("\n\n")

    click.secho("Instantiating Pythonloader", bold=True)
    pythonloader_class = get_pythonloader_class(name)
    loader = pythonloader_class(
        start_date=date_from,
        end_date=date_to,
        tenant=tenant,
        **kw_variables
    )
    loader.load()
    click.echo(success)


if __name__ == '__main__':
    pythonloader()
