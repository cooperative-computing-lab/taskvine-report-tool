import requests
import json
import sys
import argparse
import time

def test_api(base_url="http://localhost:9122", runtime_template=None, api_name=None):
    try:
        url = f"{base_url}/api/change-runtime-template"
        params = {"runtime_template": runtime_template}
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if not data['success']:
            print(f"Error: {data}")
            sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

    try:
        url = f"{base_url}/api/{api_name}"
        params = {"runtime_template": runtime_template}

        time_start = time.time()
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        time_end = time.time()
        print(f"Time taken: {round(time_end - time_start, 4)} seconds")
        with open('api_response.json', 'w') as f:
            json.dump(data, f, indent=2)
        print("Success, full response saved to 'api_response.json'")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('runtime_template', help='Specific runtime template to query')
    parser.add_argument('--port', default=9122, help='Port number')
    args = parser.parse_args()
    
    base_url = f"http://localhost:{args.port}"
    
    api_list = [
        "runtime-templates-list",
        "execution-details",
        "storage-consumption",
        "worker-transfers",
        "task-execution-time"
    ]
    testing_api = api_list[-1]

    test_api(base_url, runtime_template=args.runtime_template, api_name=testing_api)

