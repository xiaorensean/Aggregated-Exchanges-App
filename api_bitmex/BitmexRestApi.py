import requests
import os
import sys

BASE_URL = "https://www.bitmex.com/api/v1"

def get_instrument():
    endpoint = BASE_URL + "/instrument/active"
    response = requests.get(endpoint)
    data = response.json()
    return data

if __name__ == "__main__":
    all_instruments = get_instrument()
