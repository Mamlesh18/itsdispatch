from flask import Flask, jsonify
import json
import os
import time

app = Flask(__name__)

MERGED_DATA_PATH = './merged_data.json'
ITSLOADNEW_PATH = './itsloadnew.json'

# Load the JSON data from a file
def load_data(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            return json.load(file)
    return []

# Save data to a JSON file
def save_data(file_path, data):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

# Remove duplicates based on Load ID
def remove_duplicates(data):
    seen_ids = set()
    unique_loads = []
    for load in data:
        load_id = load.get('Load ID')
        if load_id not in seen_ids:
            seen_ids.add(load_id)
            unique_loads.append(load)
    
    return unique_loads

# Update and synchronize the itsloadnew.json file with changes in merged_data.json
def synchronize_data():
    merged_data = load_data(MERGED_DATA_PATH)
    unique_merged_data = remove_duplicates(merged_data)

    itsloadnew_data = load_data(ITSLOADNEW_PATH)
    new_data = []

    # Track changes
    loads_added = 0
    loads_updated = 0
    loads_deleted = 0

    for load in unique_merged_data:
        existing_load = next((item for item in itsloadnew_data if item['Load ID'] == load['Load ID']), None)
        if existing_load:
            # Update specific fields if necessary
            if (load['Origin City'] != existing_load['Origin City'] or
                load['Origin State'] != existing_load['Origin State'] or
                load['Destination City'] != existing_load['Destination City'] or
                load['Destination State'] != existing_load['Destination State']):
                
                existing_load.update({
                    'Origin City': load['Origin City'],
                    'Origin State': load['Origin State'],
                    'Destination City': load['Destination City'],
                    'Destination State': load['Destination State']
                })
                loads_updated += 1
                print(f"Updated Load ID: {load['Load ID']}")
        else:
            # New load found, add it
            new_data.append(load)
            loads_added += 1
            print(f"Added new Load ID: {load['Load ID']}")

    # Identify deleted loads
    updated_load_ids = {load['Load ID'] for load in unique_merged_data}
    remaining_loads = []
    for load in itsloadnew_data:
        if load['Load ID'] not in updated_load_ids:
            loads_deleted += 1
            print(f"Deleted Load ID: {load['Load ID']}")
        else:
            remaining_loads.append(load)

    # Add new data to remaining_loads and save it
    remaining_loads.extend(new_data)
    save_data(ITSLOADNEW_PATH, remaining_loads)

    # Print total number of loads
    print(f"Total loads after synchronization: {len(remaining_loads)}")
    print(f"Loads added: {loads_added}, Loads updated: {loads_updated}, Loads deleted: {loads_deleted}")

@app.route('/loads', methods=['GET'])
def get_loads():
    # Synchronize data before returning the latest loads
    synchronize_data()
    data = load_data(ITSLOADNEW_PATH)
    return jsonify(data)

if __name__ == '__main__':
    # Run the Flask app and synchronization process in parallel
    app.run(debug=True)
