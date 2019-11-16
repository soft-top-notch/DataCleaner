import json

HEADER_FILE = 'headers/headers.json'


def read_header_file():
    with open(HEADER_FILE, 'r') as f:
        data = f.read()
        return json.loads(data)
