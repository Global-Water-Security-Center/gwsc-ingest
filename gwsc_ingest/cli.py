import argparse

from .era5.cli import add_era5_parsers


def create_gwsc_command_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='Commands', dest='sub-command')
    subparsers.required = True

    add_era5_parsers(subparsers)

    return parser


def gwsc_command():
    parser = create_gwsc_command_parser()
    args = parser.parse_args()
    args.func(args)
