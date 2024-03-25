import time
import requests
import pandas as pd

def application_retriever(planningAuthority = None):
    # Accepts an optional planningAuthority field (e.g. "Galway County Council")
    # Returns dataframe of all applicaitons
    # URL to the FeatureServer
    
    url = "https://services.arcgis.com/NzlPQPKn5QF9v2US/ArcGIS/rest/services/IrishPlanningApplications/FeatureServer/0/query"

    # Initialize parameters for pagination
    offset = 0
    record_count = 1000  # Number of records to fetch per request
    all_attributes = []
    params_where = "1=1" if not planningAuthority else planningAuthority

    print("About to begin fetching requests (this could take a few minutes)")

    while True:
        # Update query parameters for pagination
        params = {
            "where": params_where,
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

                # Check if there's geometry data and extract latitude and longitude
                geometry = feature.get('geometry', None)
                if geometry:
                    # Assuming point data; adjust as necessary for other types like polygons
                    latitude = geometry.get('y', None)
                    longitude = geometry.get('x', None)
                    
                    # Add latitude and longitude to the attributes with new keys
                    attributes['Latitude'] = latitude
                    attributes['Longitude'] = longitude

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

            print("Received ", len(all_attributes), " records", end="\r", flush=True)
            
            # Update the offset for the next request
            offset += record_count

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

    # Create a DataFrame from the collected attributes and return it
    return pd.DataFrame(all_attributes)