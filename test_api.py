import requests
import json
import sys
import argparse
import time

def test_api(base_url="http://localhost:9122", runtime_template=None, api_name=None):
    try:
        # First change the runtime template
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
        if api_name == "worker-transfers":
            # Test incoming transfers
            url = f"{base_url}/api/worker-transfers"
            params = {"type": "incoming", "runtime_template": runtime_template}
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            save_api_response('worker-transfers-incoming', data)
            print("Saved incoming transfers response")

            # Test outgoing transfers
            params["type"] = "outgoing"
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            save_api_response('worker-transfers-outgoing', data)
            print("Saved outgoing transfers response")
        else:
            url = f"{base_url}/api/{api_name}"
            params = {"runtime_template": runtime_template}
            time_start = time.time()
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            time_end = time.time()
            print(f"Time taken: {round(time_end - time_start, 4)} seconds")
            save_api_response(api_name, data)
            print("Success, full response saved to 'api_response.json'")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

def test_task_concurrency(client):
    response = client.get('/api/task-concurrency')
    assert response.status_code == 200
    data = response.json

    # Check basic structure
    assert all(key in data for key in [
        'tasks_waiting',
        'tasks_committing',
        'tasks_executing',
        'tasks_retrieving',
        'tasks_done',
        'xMin',
        'xMax',
        'yMin',
        'yMax',
        'xTickValues',
        'yTickValues',
        'tickFontSize'
    ])

    # Test with specific task types
    selected_types = 'tasks_waiting,tasks_executing'
    response = client.get(f'/api/task-concurrency?types={selected_types}')
    assert response.status_code == 200
    data = response.json

    # Check that all task types exist in response but only selected ones have data
    for task_type in ['tasks_waiting', 'tasks_committing', 'tasks_executing', 'tasks_retrieving', 'tasks_done']:
        assert task_type in data
        if task_type in selected_types.split(','):
            # Selected types might have data (empty list is also valid)
            assert isinstance(data[task_type], list)
        else:
            # Non-selected types should have empty lists
            assert data[task_type] == []

    # Check data format for time series
    for task_type in selected_types.split(','):
        if data[task_type]:  # if there's data
            for point in data[task_type]:
                assert len(point) == 2  # Each point should be [time, value]
                assert isinstance(point[0], (int, float))  # time
                assert isinstance(point[1], (int, float))  # value

    # Check tick values
    assert len(data['xTickValues']) == 5  # Should have 5 tick values
    assert len(data['yTickValues']) == 5
    assert all(isinstance(x, (int, float)) for x in data['xTickValues'])
    assert all(isinstance(y, (int, float)) for y in data['yTickValues'])

    # Check bounds
    assert data['xMin'] <= data['xMax']
    assert data['yMin'] <= data['yMax']
    assert isinstance(data['tickFontSize'], (int, float))

def test_worker_transfers(client, runtime_template):
    # First change the runtime template
    response = client.get(f'/api/change-runtime-template?runtime_template={runtime_template}')
    assert response.status_code == 200
    assert response.json['success']

    # Test incoming transfers
    response = client.get('/api/worker-transfers?type=incoming')
    assert response.status_code == 200
    data = response.json
    save_api_response('worker-transfers-incoming', data)

    # Test outgoing transfers
    response = client.get('/api/worker-transfers?type=outgoing')
    assert response.status_code == 200
    data = response.json
    save_api_response('worker-transfers-outgoing', data)

    # Basic structure checks for both responses
    for response_data in [data]:
        assert all(key in response_data for key in [
            'transfers',
            'xMin',
            'xMax',
            'yMin',
            'yMax',
            'xTickValues',
            'yTickValues',
            'tickFontSize'
        ])

        # Check data format
        assert isinstance(response_data['transfers'], dict)
        for worker, transfers in response_data['transfers'].items():
            assert ':' in worker  # Check worker ID format
            assert isinstance(transfers, list)
            for point in transfers:
                assert len(point) == 2
                assert isinstance(point[0], (int, float))  # time
                assert isinstance(point[1], (int, float))  # value

        # Check tick values and bounds
        assert len(response_data['xTickValues']) == 5
        assert len(response_data['yTickValues']) == 5
        assert all(isinstance(x, (int, float)) for x in response_data['xTickValues'])
        assert all(isinstance(y, (int, float)) for y in response_data['yTickValues'])
        assert response_data['xMin'] <= response_data['xMax']
        assert response_data['yMin'] <= response_data['yMax']
        assert isinstance(response_data['tickFontSize'], (int, float))

    # Test invalid transfer type
    response = client.get('/api/worker-transfers?type=invalid')
    assert response.status_code == 400

def save_api_response(api_name, data):
    """Save API response to a JSON file with proper formatting.
    
    Args:
        api_name (str): Name of the API endpoint
        data (dict): Response data to save
    """
    try:
        # Create a dictionary with the API name as key
        response_data = {api_name: data}
        
        # Try to load existing data if file exists
        try:
            with open('api_response.json', 'r') as f:
                existing_data = json.load(f)
                existing_data.update(response_data)
                response_data = existing_data
        except (FileNotFoundError, json.JSONDecodeError):
            pass  # Use only new data if file doesn't exist or is invalid
        
        # Save the updated data
        with open('api_response.json', 'w') as f:
            json.dump(response_data, f, indent=2)
            
    except Exception as e:
        print(f"Error saving API response: {e}")

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
        "task-execution-time",
        "task-concurrency",
        "worker-transfers",
    ]
    testing_api = api_list[-1]  # 测试 worker-transfers

    test_api(base_url, runtime_template=args.runtime_template, api_name=testing_api)

