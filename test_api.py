import requests
import json
import sys

def test_execution_details_api(base_url="http://localhost:9122", runtime_template=None):
    """Test the execution details API endpoint"""
    
    # Construct URL with optional log_dir parameter
    url = f"{base_url}/api/execution-details"
    params = {"runtime_template": runtime_template} if runtime_template else {}
    
    try:
        # Make the request
        response = requests.get(url, params=params)
        
        # Check if request was successful
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()
        
        # Print the structure and some basic stats
        print("API Response Structure:")
        print("-" * 50)
        
        if 'error' in data:
            print(f"Error: {data['error']}")
            return
        
        if 'tasksDone' in data:
            print(f"Successful Tasks: {len(data['tasksDone'])}")
        else:
            print("No successful tasks")
        
        if 'taskFailedOnWorker' in data:
            print(f"Failed Tasks: {len(data['taskFailedOnWorker'])}")
        else:
            print("No failed tasks")
        
        if 'workerSummary' in data:
            print(f"Number of Workers: {len(data['workerSummary'])}")
        
        if 'managerInfo' in data:
            print("\nManager Info:")
            print(json.dumps(data['managerInfo'], indent=2))
        else:
            print("No manager info")
        
        # Save full response to file for detailed inspection
        with open('api_response.json', 'w') as f:
            json.dump(data, f, indent=2)
        print("\nFull response saved to 'api_response.json'")
        
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        sys.exit(1)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('runtime_template', help='Specific runtime template to query')
    parser.add_argument('--port', default=9122, help='Port number')
    args = parser.parse_args()
    
    base_url = f"http://localhost:{args.port}"
    test_execution_details_api(base_url, args.runtime_template)
