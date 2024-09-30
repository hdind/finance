from dotenv import load_dotenv, find_dotenv
import subprocess
import os


_ = load_dotenv(find_dotenv())

# Root folder path
ROOT = os.getenv("ROOT")

# Environment
branch = subprocess.check_output(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        stderr=subprocess.STDOUT
    ).strip().decode("utf-8")
ENV = "prd" if branch == "main" else "dev"

# PostreSQL connection
PSQL_CONN = {
    "psql_db": "finance" if ENV == "prd" else "finance_dev",
    "psql_username": "prd_user" if ENV == "prd" else "dev_user",
    "psql_password": os.getenv("PSQL_PASSWORD")
}

PDF_PASSWORD = os.getenv('PDF_PASSWORD')
