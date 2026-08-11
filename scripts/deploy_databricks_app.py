#!/usr/bin/env python3

import argparse
import subprocess


SOURCE_CODE_PATH = (
    "/Workspace/Shared/Databricks Support/watersync/app/watersync-control-plane"
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy the WaterSync Databricks App")
    parser.add_argument("--profile", required=True, help="Databricks CLI profile")
    parser.add_argument("--app-name", default="watersync-control-plane")
    args = parser.parse_args()

    subprocess.run(
        [
            "databricks",
            "apps",
            "deploy",
            args.app_name,
            "--source-code-path",
            SOURCE_CODE_PATH,
            "--mode",
            "SNAPSHOT",
            "--profile",
            args.profile,
            "--auto-approve",
        ],
        check=True,
    )

    subprocess.run(
        [
            "databricks",
            "apps",
            "get",
            args.app_name,
            "--profile",
            args.profile,
            "--output",
            "json",
        ],
        check=True,
    )


if __name__ == "__main__":
    main()
