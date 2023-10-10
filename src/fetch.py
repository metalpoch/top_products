from os import environ
from uuid import uuid4
from base64 import b64encode
from dotenv import load_dotenv
import requests


load_dotenv()

CLIENT_ID = environ.get("CLIENT_ID")
CLIENT_SECRET = environ.get("CLIENT_SECRET")
AUTH_STRING = b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()

HEADERS = {
    "Authorization": f"Basic {AUTH_STRING}",
    "WM_QOS.CORRELATION_ID": str(uuid4()),
    "WM_SVC.NAME": "Walmart Marketplace",
    "Accept": "application/json",
}


def auth_token() -> str:
    headers = {**HEADERS, "Content-Type": "application/x-www-form-urlencoded"}
    try:
        res = requests.post(
            "https://marketplace.walmartapis.com/v3/token",
            data={"grant_type": "client_credentials"},
            headers=headers,
        )
    except requests.exceptions.ConnectionError:
        print("Failed to establish a new connection")
        return ""
    else:
        return res.json()["access_token"] if res.status_code == 200 else ""


def get(url: str, token: str = "") -> requests.Response:
    if token == "":
        headers = {**HEADERS, "WM_SEC.ACCESS_TOKEN": auth_token()}
    else:
        headers = {**HEADERS, "WM_SEC.ACCESS_TOKEN": token}

    while True:
        try:
            res = requests.get(url, headers=headers)
        except requests.exceptions.ConnectionError:
            print("Failed to establish a new connection")
        else:
            return res
