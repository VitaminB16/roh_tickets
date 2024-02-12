import os
from dotenv import load_dotenv

def set_secrets():
    """
    Set environment variables from secrets.txt

    secrets.txt should be in the format:
    KEY1 = VALUE1
    KEY2 = VALUE2
    """
    with open("secrets.txt") as f:
        for line in f:
            key, value = line.strip().split(" = ")
            os.environ[key] = value

load_dotenv()
set_secrets()
