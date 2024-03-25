import requests

# URL to the FeatureServer
url = "https://services.arcgis.com/NzlPQPKn5QF9v2US/ArcGIS/rest/services/IrishPlanningApplications/FeatureServer/0/query"

# Initialize parameters for pagination and counting
offset = 0
record_count = 1000  # Number of records to fetch per request
total_count = 0  # To keep track of the total number of records

print("About to begin fetching requests")

while True:
    # Update query parameters for pagination and filtering by PlanningAuthority
    params = {
        "where": "PlanningAuthority='Galway County Council'",
        "outFields": "PlanningAuthority",  # Only fetch the PlanningAuthority field
        "f": "json",
        "resultOffset": offset,
        "resultRecordCount": record_count
    }

    # Send a GET request to the server
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()

        # Extract the 'features' part of the response
        features = data.get('features', [])

        if not features:
            break  # Break the loop if no more features are returned

        # Count the number of features (records) returned in this batch
        batch_count = len(features)
        total_count += batch_count

        # Update the offset for the next request
        offset += record_count
        print(f"Retrieved {offset} responses, total count so far: {total_count}...")
    else:
        print("Failed to retrieve data: Status code", response.status_code)
        break

print(f"Total records from Galway County Council: {total_count}")
