import json
from typing import Optional, Dict, Any, List
from backend.bigquery_connector import BigQueryConnector
import re


def get_dataset_and_table_details(project_id: str, dataset_name: str, region: str) -> str:
    """
    Retrieves comprehensive details for a single BigQuery dataset, including
    its DDL, and detailed information for all its tables (storage, partitioning, etc.).

    Args:
        project_id: The GCP project ID.
        dataset_name: The name of the dataset.
        region: The GCP region where the dataset resides.

    Returns:
        A JSON string containing the structured details for the dataset.
    """
    try:
        connector = BigQueryConnector(project_id=project_id, region=region)
        print(f"--- Fetching comprehensive details for dataset: {dataset_name} in region {region} ---")

        # 1. Get Dataset DDL - This is a region-scoped view
        dataset_ddl_query = f"SELECT ddl FROM `{project_id}`.`region-{region}`.INFORMATION_SCHEMA.SCHEMATA WHERE schema_name = '{dataset_name}'"
        dataset_ddl_result = connector.execute_query(dataset_ddl_query)
        dataset_ddl = dataset_ddl_result[0].get("ddl") if dataset_ddl_result else ""
        has_dataset_description = "OPTIONS(description=" in dataset_ddl

        # 2. Get base table info (name, type, ddl) - This is dataset-scoped
        tables_query = f"SELECT table_name, table_type, ddl FROM `{project_id}`.{dataset_name}.INFORMATION_SCHEMA.TABLES"
        tables_results = connector.execute_query(tables_query)
        
        # Use a dictionary for quick lookups
        tables_map: Dict[str, Dict[str, Any]] = {}
        if tables_results:
            for t in tables_results:
                table_name = t["table_name"]
                ddl = t["ddl"]
                
                # Check for table description
                has_table_description = "OPTIONS(description=" in ddl

                # Calculate column description completeness
                columns_match = re.findall(r"^\s*`?(\w+)`?\s+\w+", ddl, re.MULTILINE)
                described_columns_match = re.findall(r"OPTIONS\(description=", ddl, re.IGNORECASE)
                
                # The first described column is the table itself, so we subtract it
                total_columns = len(columns_match)
                described_columns_count = max(0, len(described_columns_match) - (1 if has_table_description else 0))

                column_completeness = 0
                if total_columns > 0:
                    column_completeness = round(described_columns_count / total_columns, 2)

                tables_map[table_name] = {
                    "table_name": table_name, 
                    "table_type": t["table_type"], 
                    "ddl": ddl,
                    "has_table_description": has_table_description,
                    "column_description_completeness": column_completeness
                }


        # 3. Get table storage info - This is region-scoped
        storage_query = f"SELECT table_name, total_rows, total_logical_bytes, total_physical_bytes FROM `{project_id}`.`region-{region}`.INFORMATION_SCHEMA.TABLE_STORAGE WHERE table_schema = '{dataset_name}'"
        storage_results = connector.execute_query(storage_query)
        if storage_results:
            for row in storage_results:
                if row["table_name"] in tables_map:
                    tables_map[row["table_name"]].update({
                        "rows": row["total_rows"],
                        "logical_gb": round(row["total_logical_bytes"] / (1024**3), 2) if row.get("total_logical_bytes") else 0,
                        "billable_gb": round(row["total_physical_bytes"] / (1024**3), 2) if row.get("total_physical_bytes") else 0,
                    })

        # 4. Get last modified time from partitions - This is dataset-scoped
        partitions_query = f"SELECT table_name, MAX(last_modified_time) as last_modified_time FROM `{project_id}`.{dataset_name}.INFORMATION_SCHEMA.PARTITIONS GROUP BY table_name"
        partitions_results = connector.execute_query(partitions_query)
        if partitions_results:
            for row in partitions_results:
                 if row["table_name"] in tables_map:
                    # Convert timestamp to string if it exists
                    last_modified = row["last_modified_time"]
                    tables_map[row["table_name"]]["last_modified"] = last_modified.isoformat() if last_modified else None

        # 5. Get table options (partitioning, clustering) - This is dataset-scoped
        options_query = f"SELECT table_name, option_name, option_value FROM `{project_id}`.{dataset_name}.INFORMATION_SCHEMA.TABLE_OPTIONS"
        options_results = connector.execute_query(options_query)
        if options_results:
            for row in options_results:
                if row["table_name"] in tables_map:
                    if "partition" in row["option_name"]:
                         tables_map[row["table_name"]]["partitioning_info"] = row["option_value"]
                    if "clustering" in row["option_name"]:
                         tables_map[row["table_name"]]["clustering_info"] = row["option_value"]

        # Final Assembly
        dataset_details = {
            "schema_name": dataset_name,
            "ddl": dataset_ddl,
            "has_dataset_description": has_dataset_description,
            "tables": list(tables_map.values()) # Convert map back to list
        }
        
        return json.dumps(dataset_details, default=str) # Use default=str for datetime fallback

    except Exception as e:
        error_message = f"An error occurred while analyzing dataset `{dataset_name}`: {e}"
        print(error_message)
        # Return a consistent error format
        return json.dumps({"error": error_message})


# The old functions are kept for now to avoid breaking changes, but are now deprecated.
# They will be removed once the refactoring is complete.

def get_dataset_ddl(project_id: str, dataset_name: str) -> Optional[str]:
    """
    DEPRECATED. Use get_dataset_and_table_details instead.
    Retrieves the DDL for a single BigQuery dataset.

    Args:
        project_id: The GCP project ID.
        dataset_name: The name of the dataset.

    Returns:
        The DDL string for the dataset, or None if not found.
    """
    query = f"SELECT ddl FROM `{project_id}`.INFORMATION_SCHEMA.SCHEMATA WHERE schema_name = '{dataset_name}'"
    try:
        print(f"--- Executing DDL query for dataset '{dataset_name}' ---")
        connector = BigQueryConnector(project_id=project_id)
        results = connector.execute_query(query)
        if results and isinstance(results, list) and len(results) > 0:
            return results[0].get("ddl")
        return None
    except Exception as e:
        print(f"Could not retrieve DDL for dataset {dataset_name}: {e}")
        return None


def get_dataset_details(project_id: str, dataset_name: str) -> str:
    """
    DEPRECATED. Use get_dataset_and_table_details instead.
    Retrieves table metadata for a specific BigQuery dataset.
    This tool gets table names and DDL statements, which are used for
    parsing table and column descriptions.

    Args:
        project_id: The GCP project ID.
        dataset_name: The name of the dataset to analyze.

    Returns:
        A JSON string representation of the query results, or an error message.
        Each item in the list is an object with table_name and ddl.
    """
    # This query retrieves the DDL for all tables in the dataset.
    query_tables = f"SELECT table_name, ddl FROM `{project_id}`.{dataset_name}.INFORMATION_SCHEMA.TABLES"

    try:
        print(f"--- Executing TABLE query for dataset '{dataset_name}' ---")
        connector = BigQueryConnector(project_id=project_id)

        tables_results = connector.execute_query(query_tables)

        # Return results as a JSON string for safe parsing.
        return json.dumps(tables_results if tables_results else [])
    except Exception as e:
        error_message = f"An error occurred while analyzing dataset `{dataset_name}`: {e}"
        if "Not found" in str(e):
            error_message = f"Dataset `{dataset_name}` in project `{project_id}` not found or permission denied."
        # Always return a JSON object with an error key
        return json.dumps({"error": error_message})


def execute_bigquery_query(query: str, region: Optional[str] = None) -> str:
    """
    Executes a SQL query against Google BigQuery.
    If a region is specified, it targets the regional INFORMATION_SCHEMA.

    Args:
        query: The SQL query to be executed.
        region: (Optional) The GCP region for the query.

    Returns:
        A string representation of the query results, or an error message.
    """
    try:
        # Extract project_id from the query if it contains one
        # Look for patterns like `project_id.INFORMATION_SCHEMA` or `project_id.dataset_name`
        import re
        project_match = re.search(r'`?([a-zA-Z0-9\-_]+)`?\.(?:INFORMATION_SCHEMA|`?[a-zA-Z0-9\-_]+`?)', query)
        project_id = None
        if project_match:
            project_id = project_match.group(1)
        
        # Create connector with the extracted project_id and region
        connector = BigQueryConnector(project_id=project_id, region=region)
        results = connector.execute_query(query)
        return json.dumps([dict(row) for row in results])
    except Exception as e:
        return f'{{"error": "An error occurred: {e}"}}'

def perform_google_search(query: str) -> str:
    """
    Use this tool to search Google for up-to-date information, documentation, or best practices.

    Args:
        query: The search query. Be specific and include terms like "Google Cloud",
               "BigQuery", the name of a service, or a specific error message.

    Returns:
        A string containing the search results.
    """
    # This function is a placeholder. The model will use its native search
    # capabilities when it sees this tool signature.
    pass 

def discover_datasets_across_regions(project_id: str) -> str:
    """
    Automatically discovers datasets across multiple regions in a BigQuery project.
    This function tries common regions and multi-regions to find all datasets.
    
    Args:
        project_id: The GCP project ID.
        
    Returns:
        A JSON string containing all discovered datasets with their regions.
    """
    # Common regions and multi-regions to try
    regions_to_try = [
        "US",           # US multi-region
        "EU",           # European multi-region
        "asia-northeast1",  # Asia region
        "us-central1",  # US Central
        "us-east1",     # US East
        "europe-west1", # Europe West
        "asia-southeast1"  # Asia Southeast
    ]
    
    all_datasets = []
    discovered_regions = {}
    
    for region in regions_to_try:
        try:
            connector = BigQueryConnector(project_id=project_id, region=region)
            query = f"SELECT schema_name FROM `{project_id}`.INFORMATION_SCHEMA.SCHEMATA"
            results = connector.execute_query(query)
            
            if results:
                for row in results:
                    dataset_name = row.get("schema_name")
                    if dataset_name and not any(d["schema_name"] == dataset_name for d in all_datasets):
                        all_datasets.append({
                            "schema_name": dataset_name,
                            "region": region
                        })
                discovered_regions[region] = len(results)
                print(f"Found {len(results)} datasets in region {region}")
            
        except Exception as e:
            print(f"Could not query region {region}: {e}")
            continue
    
    # Sort datasets by name for consistency
    all_datasets.sort(key=lambda x: x["schema_name"])
    
    result = {
        "datasets": all_datasets,
        "total_datasets": len(all_datasets),
        "regions_checked": list(discovered_regions.keys()),
        "datasets_per_region": discovered_regions
    }
    
    return json.dumps(result) 