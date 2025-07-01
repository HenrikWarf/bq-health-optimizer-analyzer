import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

class BigQueryConnector:
    """
    A class to handle the connection to Google BigQuery and execute queries.
    """
    def __init__(self, project_id: str = None, region: str = None):
        """
        Initializes the BigQuery client.
        
        Args:
            project_id: The GCP project ID. If None, defaults to the one in
                        the GOOGLE_CLOUD_PROJECT environment variable.
            region: The GCP region to connect to. If None, defaults to the
                    region specified in the GOOGLE_CLOUD_REGION environment variable.
        """
        self.project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        # Use the provided region, or fall back to the environment variable.
        self.region = region or os.getenv("GOOGLE_CLOUD_REGION")
        
        if not self.project_id or not self.region:
            raise ValueError("GOOGLE_CLOUD_PROJECT and GOOGLE_CLOUD_REGION must be set.")
        
        self.client = bigquery.Client(project=self.project_id, location=self.region)

    def execute_query(self, query: str):
        """
        Executes a SQL query in BigQuery and returns the results as a list of dicts.

        Args:
            query: The SQL query string to execute.

        Returns:
            A list of rows, where each row is a dictionary-like object.
            Returns an empty list if the query fails or returns no results.
        """
        try:
            # The client is initialized with the correct location (region),
            # so BigQuery will route the query to the appropriate endpoint.
            # No need to specify the region in the SQL string itself.
            query_job = self.client.query(query)  # API request.
            results = query_job.result()  # Waits for the job to complete.
            return [dict(row) for row in results]
        except Exception as e:
            # Log the error for server-side debugging, but also raise it so the
            # calling tool can handle it and report it to the agent.
            print(f"An error occurred while executing the query: {e}")
            raise e

if __name__ == '__main__':
    # Example usage:
    # Ensure you have a .env file with GOOGLE_CLOUD_PROJECT set
    # and you have authenticated with `gcloud auth application-default login`
    
    connector = BigQueryConnector()
    
    # Example query to get all datasets in the project
    # Note: You need to specify a region in the query for INFORMATION_SCHEMA
    # This example assumes your resources are in the 'US' multi-region.
    # Replace 'region-us' with the appropriate region if necessary.
    
    example_query = f"""
    SELECT schema_name
    FROM INFORMATION_SCHEMA.SCHEMATA
    LIMIT 10;
    """
    
    print(f"Executing query on project: {connector.project_id} in region: {connector.region}")
    query_results = connector.execute_query(example_query)
    
    if query_results:
        print("Successfully retrieved datasets:")
        for row in query_results:
            print(row['schema_name'])
    else:
        print("Could not retrieve datasets. Please check your configuration and permissions.") 