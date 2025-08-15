# SPDX-FileCopyrightText: LakeSoul Contributors

# SPDX-License-Identifier: Apache-2.0


from e2etest.core import cli, run, check


def main() -> None:
    cli.add_command(run)
    cli.add_command(check)
    cli(obj={})


if __name__ == "__main__":
    main()
