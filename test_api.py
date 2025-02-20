import requests
import json
import sys
import argparse


def test_api(base_url="http://localhost:9122", runtime_template=None, api_name="storage-consumption"):
    url = f"{base_url}/api/{api_name}"
    params = {"runtime_template": runtime_template} if runtime_template else {}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        with open('api_response.json', 'w') as f:
            json.dump(data, f, indent=2)
        print("\nFull response saved to 'api_response.json'")
        
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('runtime_template', help='Specific runtime template to query')
    parser.add_argument('--port', default=9122, help='Port number')
    args = parser.parse_args()
    
    base_url = f"http://localhost:{args.port}"
    # test_api(base_url, args.runtime_template, "execution-details")
    test_api(base_url, args.runtime_template, "storage-consumption")
