import pandas as pd
from sqlalchemy import create_engine
import yaml

# Function to load credentials from the YAML file
def load_credentials(file_path):
    try:
        with open(file_path, 'r') as file:
            credentials_data = yaml.safe_load(file)
        return credentials_data
    except FileNotFoundError:
        print(f"Error: File not found - {file_path}")
        return None
    except yaml.YAMLError as e:
        print(f"Error loading YAML file: {e}")
        return None

class RDSDatabaseConnector:
    def __init__(self, credentials):
        self.credentials = credentials
        self.engine = self.create_engine()

    def create_engine(self):
        try:
            # Extract necessary credentials from self.credentials
            database_type = 'postgresql'  # Hardcoded as it's specific to PostgreSQL
            dbapi = 'psycopg2'  # Hardcoded as it's specific to PostgreSQL
            endpoint = self.credentials.get('RDS_HOST')
            user = self.credentials.get('RDS_USER')
            password = self.credentials.get('RDS_PASSWORD')
            port = self.credentials.get('RDS_PORT')
            database_name = self.credentials.get('RDS_DATABASE')
            # Check if any key is missing
            if None in [endpoint, user, password, port, database_name]:
                raise ValueError("One or more required keys are missing in credentials.")

            # Create and return a SQLAlchemy engine
            engine = create_engine(f"{database_type}+{dbapi}://{user}:{password}@{endpoint}:{port}/{database_name}")
            return engine
        except Exception as e:
            print(f"Error creating engine: {e}")
            return None

    def extract_data(self):
        try:
            # Replace 'your_query_here' with the actual SQL query
            query = "SELECT * FROM loan_payments"
            df = pd.read_sql(query, self.engine)
            return df
        except Exception as e:
            print(f"Error extracting data from the database: {e}")
            return None

    def save_to_csv(self, file_path):
        try:
            # Extract data from the database
            data_frame = self.extract_data()  # No arguments here

            # Check if the data_frame is not None
            if data_frame is not None:
                # Save the DataFrame to a CSV file
                data_frame.to_csv(file_path, index=False)
                print(f"Data successfully saved to {file_path}")
            else:
                print("Unable to save data. Data extraction failed.")
        except Exception as e:
            print(f"Error saving data to CSV: {e}")

    def load_from_csv(self, file_path):
        try:
            # Use Pandas to read the CSV file into a DataFrame
            data_frame = pd.read_csv(file_path)
            print(f"Data successfully loaded from {file_path}")
            return data_frame
        except FileNotFoundError:
            print(f"Error: File not found - {file_path}")
            return None
        except Exception as e:
            print(f"Error loading data from CSV: {e}")
            return None

# usage to load rds
credentials_path = 'c:\\Users\\Laura\\EDA-Finance\\credentials.yaml'
credentials_data = load_credentials(credentials_path)

if credentials_data:
    rds_connector = RDSDatabaseConnector(credentials_data)

    # Specify the desired file path for saving the CSV file
    csv_file_path = 'C:/Users/Laura/EDA-Finance/loan_payments_data.csv'

    # Call the save_to_csv function
    rds_connector.save_to_csv(csv_file_path)  # Replace 'your_query_here' with your actual SQL query

    # Call the load_from_csv function
    loaded_data = rds_connector.load_from_csv(csv_file_path)

    # Display the shape of the data (number of rows and columns)
    print("Data Shape:", loaded_data.shape)

    # Display a sample of the data (e.g., the first 5 rows)
    print("\nSample Data:")
    print(loaded_data.head())
