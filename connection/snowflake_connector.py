import snowflake.connector
import os
from dotenv import load_dotenv
import pathlib
import utils.logger as Logger

# extract the path of .env file
dotenv_path = os.path.join(pathlib.Path(__file__).parent.parent.resolve(), "config\\.env")
load_dotenv(dotenv_path) # dotenv_path: Absolute or relative path to .env file. Parse a .env file and then load all the variables found as environment variables.

def connection():
    sf_connector = snowflake.connector.connect(
        user = os.getenv('USER'),
        password = os.getenv('PASSWORD'),
        account = os.getenv('ACCOUNT'),
        # database = os.getenv('DB')
    )

    sf_cursor = sf_connector.cursor() # Creates a cursor object. Each statement will be executed in a new cursor object.

    Logger.log_info("[SUCCESS] Establish connection to snowflake")
    return sf_cursor