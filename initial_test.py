import requests
import pandas as pd
import time

# URL to the FeatureServer
url = "https://services.arcgis.com/NzlPQPKn5QF9v2US/ArcGIS/rest/services/IrishPlanningApplications/FeatureServer/0/query"

# Initialize parameters for pagination
offset = 0
record_count = 1000  # Number of records to fetch per request
all_attributes = []

print("About to begin fetching requests")

while True:
    # Update query parameters for pagination
    params = {
        "where": "PlanningAuthority='Galway County Council'",
        "outFields": "*",
        "f": "json",
        "resultOffset": offset,
        "resultRecordCount": record_count
    }

    try:
        # Send a GET request to the server
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raises a HTTPError for bad responses

        data = response.json()
        features = data.get('features', [])

        if not features:
            break  # Exit the loop if no more features are returned

        for feature in features:
            attributes = feature['attributes'] 
            for key, value in list(attributes.items()):
                if "date" in key.lower() and value is not None:  # Check for "date" and ensure value is not None
                    try:
                        # Using milliseconds ('ms'). Adjust seconds ('s') if this doesn't work)
                        attributes[key] = pd.to_datetime(value, unit='ms', errors='coerce').strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        # If there's a ValueError, it might not be a valid timestamp
                        print(f"Conversion failed for {key} with value {value}. Keeping original value.")
                    except TypeError:
                        # If there's a TypeError, the value might not be convertible (e.g., a string that's not a timestamp)
                        print(f"Type error for {key} with value {value}. Keeping original value.")
                    except OverflowError:
                        # Handle cases where the timestamp is out of bounds for pandas to_datetime()
                        print(f"Overflow error for {key} with value {value}. Keeping original value.")

            all_attributes.append(attributes)

        # Update the offset for the next request
        offset += record_count
        print(f"Retrieved {offset} responses...")

        # Sleep between requests to avoid hitting rate limits
        time.sleep(1)  # Adjust the sleep time as needed

    except requests.exceptions.HTTPError as e:
        # Check if the error is due to rate limiting
        if e.response.status_code == 429:
            print("Rate limit hit, waiting before retrying...")
            time.sleep(10)  # Longer sleep time upon hitting rate limit
        else:
            print(f"HTTP error occurred: {e}")
            break
    except requests.exceptions.RequestException as e:
        # For other types of request-related errors
        print(f"Error fetching data: {e}")
        break

# Create a DataFrame from the collected attributes
df = pd.DataFrame(all_attributes)

# Save the DataFrame to a CSV file
df.to_csv("IrishPlanningApplications_GalwayCountyCouncil.csv", index=False)
print("Data saved to IrishPlanningApplications_GalwayCountyCouncil.csv")
