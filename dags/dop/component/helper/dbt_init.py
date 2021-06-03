import argparse
import logging
import os

from dop.component.helper import dbt_profile

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description="Process arguments")
    parser.add_argument(
        "--tmp_dir",
        required=True,
        type=str,
        help="the TMP DIR where the python virtual environment is created",
    )
    parser.add_argument(
        "--project_name", required=True, type=str, help="DBT project name"
    )

    args = parser.parse_args()

    os.mkdir(os.path.sep.join([args.tmp_dir, ".dbt"]))

    logging.info(f"Creating DBT profiles.yml in {args.tmp_dir}")
    dbt_profile.setup_and_save_profiles(
        project_name=args.project_name, profile_path=args.tmp_dir
    )

    logging.info("Done.")
