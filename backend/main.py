import os
import asyncio
import uuid
import json
import re
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sse_starlette.sse import EventSourceResponse
import uvicorn
from datetime import datetime, timezone, timedelta

from google.cloud import resourcemanager_v3
from google.adk.agents import Agent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai.types import Content, Part
from backend.tools import get_dataset_and_table_details, execute_bigquery_query, perform_google_search
from pydantic import BaseModel

# Construct the path to the .env file in the project root and load it
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path=dotenv_path)

# Create FastAPI app
app = FastAPI(
    title="BigQuery Analyzer API",
    description="An API to trigger AI agent-based analysis of a BigQuery environment.",
    version="1.0.0",
)

# Add CORS middleware to allow frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins for simplicity. For production, restrict this.
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

@app.get("/api/projects")
async def get_projects():
    """Lists all accessible Google Cloud projects."""
    try:
        client = resourcemanager_v3.ProjectsClient()
        # Using search_projects is more flexible and avoids the 'parent' issue.
        request = resourcemanager_v3.SearchProjectsRequest()
        projects = client.search_projects(request=request)
        project_list = [{"project_id": project.project_id} for project in projects]
        return project_list
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list GCP projects: {e}")

def create_discovery_agent():
    """Creates the agent responsible for discovering datasets."""
    return Agent(
        name="bigquery_dataset_discoverer",
        description="An agent that finds all dataset names in a BigQuery project.",
        model="gemini-2.5-flash",
        instruction="""You have one job: find all the dataset names in a given BigQuery project.
        You will be given the project ID and region in the prompt.
        You MUST use the `execute_bigquery_query` tool.

        When you call the tool, you need to pass two arguments:
        1. `query`: Construct a SQL query to find all dataset names. It should look like this: "SELECT schema_name FROM `<project_id>`.INFORMATION_SCHEMA.SCHEMATA;" where you replace `<project_id>` with the actual project ID given to you.
        2. `region`: The GCP region to query against.

        Your final output MUST be a valid JSON string representing a list of objects.
        Each object in the list must have one key: "schema_name".
        For example: [{"schema_name": "dataset_one"}, {"schema_name": "dataset_two"}]""",
        tools=[execute_bigquery_query],
    )

def create_summary_agent():
    """Creates the agent responsible for summarizing the full analysis."""
    return Agent(
        name="summary_agent",
        model="gemini-2.5-flash",
        instruction="""You are a world-class Google Cloud BigQuery expert, specializing in performance tuning and cost optimization.
You will be given a `baseline_score` that was pre-calculated based on a set of objective rules (like missing descriptions, partitioning, etc.).
You will also be given a JSON object containing the complete metadata for a Google Cloud project.

Your task is to perform a holistic analysis and generate a final report. Use the `baseline_score` as a strong reference for your final `health_score`.
You can adjust the score slightly up or down based on your holistic analysis of the data, but you should justify any significant deviation in your "Key Findings".

**Crucially, you MUST NOT mention the term 'baseline_score' or 'pre-calculated score' in your output.** This is an internal metric for your reference only. The user should only see the final `health_score` and your analysis.

Based on everything, you MUST generate a final JSON report with three keys:
1.  "health_score": A final integer score from 0-100.
2.  "key_findings": A list of JSON objects. Each object must have three keys: "title" (a short, one-sentence summary of the finding), "details" (a markdown-formatted string with a more in-depth explanation), and "importance" (a string that is either "High", "Medium", or "Low").
3.  "recommendations": A list of JSON objects, structured just like "key_findings", with "title", "details", and "priority" ("High", "Medium", or "Low").
""",
        # This agent performs no tool calls; it only synthesizes data.
        tools=[],
    )

def create_action_plan_agent():
    """Creates an agent that can search the web to generate action plans."""
    return Agent(
        name="action_plan_generator",
        model="gemini-2.5-flash",
        description="An agent that generates detailed, actionable steps to address a specific recommendation, using web search to find the best information.",
        instruction="""You are a helpful assistant and Google Cloud expert. Your job is to create detailed, step-by-step action plans and curated reading lists based on a user's BigQuery analysis.

You will receive a **Task** and a **Context**.

**If the Task is "Generate Action Plan":**
1.  Understand the recommendation in the context of the user's project data.
2.  Use the `perform_google_search` tool to find the most up-to-date Google Cloud documentation, tutorials, or best practice guides.
3.  Synthesize this information into a clear, concrete, and markdown-formatted action plan. Start the plan with a `#### Step-by-Step Action Plan` header.
4.  Your final output MUST be only the markdown-formatted text of the action plan.

**If the Task is "Generate Reading List":**
1.  Review the full analysis context provided.
2.  Use the `perform_google_search` tool to find 2-3 high-quality articles, blog posts, or official Google Cloud documentation pages that are highly relevant to the findings and recommendations.
3.  For each link, provide a one-sentence summary explaining *why* it is relevant to the user's situation.
4.  Your final output MUST be a JSON object with a single key, "reading_list", which is a list of objects. Each object must have two keys: "url" and "summary".
   Example: `{"reading_list": [{"url": "https://...", "summary": "This article explains..."}]}`""",
        tools=[perform_google_search],
    )

def calculate_health_score(all_data: list) -> int:
    """Calculates a health score based on a set of rules."""
    score = 100
    for dataset in all_data:
        # Deduct for missing dataset description
        if not dataset.get("has_dataset_description"):
            score -= 5

        for table in dataset.get("tables", []):
            # Deduct for missing table description
            if not table.get("has_table_description"):
                score -= 2
            
            # Deduct for incomplete column descriptions
            if table.get("column_description_completeness", 0) < 0.5:
                score -= 4

            # Deduct for large, unpartitioned tables
            if table.get("billable_gb", 0) > 1 and not table.get("partitioning_info"):
                score -= 10
            
            # Deduct for stale tables
            last_modified_str = table.get("last_modified")
            if last_modified_str:
                last_modified_dt = datetime.fromisoformat(last_modified_str)
                if datetime.now(timezone.utc) - last_modified_dt > timedelta(days=90):
                    score -= 3

    return max(0, score) # Ensure score doesn't go below 0

async def run_agent(agent, initial_prompt):
    """A helper function to run an agent and return its final response."""
    app_name = "bigquery_analyzer_app"
    user_id = "default_user"
    session_id = str(uuid.uuid4())
    session_service = InMemorySessionService()

    runner = Runner(
        agent=agent, app_name=app_name, session_service=session_service
    )

    message_content = Content(role="user", parts=[Part(text=initial_prompt)])
    await session_service.create_session(app_name=app_name, user_id=user_id, session_id=session_id)

    final_response_text = ""
    events_async = runner.run_async(
        user_id=user_id, session_id=session_id, new_message=message_content
    )

    async for event in events_async:
        if event.is_final_response() and event.content and event.content.parts:
            final_response_text = "".join(part.text or "" for part in event.content.parts)
            
    if not final_response_text:
        raise Exception("Agent did not produce a final text report.")
        
    return final_response_text

class ActionPlanRequest(BaseModel):
    recommendation: dict
    analysis_context: list

@app.post("/api/generate_action_plan")
async def generate_action_plan(request_data: ActionPlanRequest):
    """Generates a detailed action plan for a specific recommendation."""
    try:
        agent = create_action_plan_agent()
        
        # Construct a detailed prompt for the agent
        prompt = f"""
        Task: Generate Action Plan

        Here is the recommendation I need an action plan for:
        Title: {request_data.recommendation.get('title')}
        Details: {request_data.recommendation.get('details')}

        Here is the full analysis context of the BigQuery project this recommendation applies to:
        {json.dumps(request_data.analysis_context, indent=2)}

        Please generate a step-by-step action plan to address this recommendation. Use your search tool to find the best, most current information.
        """
        
        action_plan_text = await run_agent(agent, prompt)
        return {"action_plan": action_plan_text}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analyze")
async def analyze_environment(request: Request):
    """
    Triggers the BigQuery analysis and streams progress updates back to the client
    using Server-Sent Events.
    """
    project_id = request.query_params.get("project_id")
    if not project_id:
        raise HTTPException(status_code=400, detail="Missing 'project_id' query parameter.")
        
    async def event_stream():
        """The generator function that yields progress events."""
        try:
            # Initial state
            yield {"event": "update", "data": json.dumps({'status': 'Starting', 'progress': 0, 'details': 'Initializing...'})}
            yield {"event": "checkpoint", "data": json.dumps({'text': 'Connecting to Google Cloud...'})}
            
            if await request.is_disconnected(): return
            region = os.getenv("GOOGLE_CLOUD_REGION")
            if not os.getenv("GEMINI_API_KEY") or not region:
                raise ValueError("Required environment variables are not set.")

            # Step 1: Discover datasets
            yield {"event": "update", "data": json.dumps({'status': 'Discovery', 'progress': 10, 'details': 'Discovering datasets...'})}
            yield {"event": "checkpoint", "data": json.dumps({'text': 'Discovering all datasets in project...'})}
            discovery_agent = create_discovery_agent()
            discovery_prompt = f"Find all datasets in the project `{project_id}` using the region `{region}`."
            dataset_list_json_str = await run_agent(discovery_agent, discovery_prompt)
            if await request.is_disconnected(): return
            
            try:
                if dataset_list_json_str.strip().startswith("```json"):
                    dataset_list_json_str = dataset_list_json_str.strip()[7:-4].strip()
                if not dataset_list_json_str.strip():
                    raise ValueError("The Discovery Agent returned an empty response.")
                discovered_datasets = json.loads(dataset_list_json_str)
            except (json.JSONDecodeError, ValueError) as e:
                raise Exception(f"Discovery Agent failed to return valid JSON. Raw output: '{dataset_list_json_str}'. Error: {e}")

            # Step 2: Gather table details
            yield {"event": "checkpoint", "data": json.dumps({'text': f'Found {len(discovered_datasets)} datasets. Fetching details...'})}
            full_environment_data = []
            total_datasets = len(discovered_datasets)
            for i, dataset_info in enumerate(discovered_datasets):
                if await request.is_disconnected(): break
                dataset_name = dataset_info.get("schema_name")
                if not dataset_name: continue
                
                progress = 20 + int((i / total_datasets) * 40)
                yield {"event": "update", "data": json.dumps({'status': 'Fetching', 'progress': progress, 'details': f'Fetching details for: {dataset_name}'})}
                details_json_str = get_dataset_and_table_details(project_id=project_id, dataset_name=dataset_name, region=region)
                dataset_details = json.loads(details_json_str)
                if isinstance(dataset_details, dict) and "error" in dataset_details:
                    print(f"Skipping dataset {dataset_name} due to error: {dataset_details['error']}")
                    continue
                full_environment_data.append(dataset_details)
            if await request.is_disconnected(): return
            
            yield {"event": "checkpoint", "data": json.dumps({'text': 'All dataset details collected.'})}

            # Step 3: Run Summary Agent
            yield {"event": "update", "data": json.dumps({'status': 'Analyzing', 'progress': 75, 'details': 'Calculating health score...', 'full_environment_data': full_environment_data})}
            yield {"event": "checkpoint", "data": json.dumps({'text': 'Calculating baseline health score...'})}
            baseline_score = calculate_health_score(full_environment_data)

            yield {"event": "update", "data": json.dumps({'status': 'Analyzing', 'progress': 85, 'details': 'Generating final report...'})}
            yield {"event": "checkpoint", "data": json.dumps({'text': 'Sending data to AI for final analysis...'})}
            summary_agent = create_summary_agent()
            summary_prompt = f"The pre-calculated baseline score for this project is {baseline_score}. Analyze the following BigQuery project metadata, using the baseline score as a strong reference, and generate a final summary report.\\nData: {json.dumps(full_environment_data, indent=2)}"
            
            final_report_json_str = await run_agent(summary_agent, summary_prompt)
            
            try:
                if final_report_json_str.strip().startswith("```json"):
                    final_report_json_str = final_report_json_str.strip()[7:-4].strip()
                final_report_obj = json.loads(final_report_json_str)
            except json.JSONDecodeError as e:
                raise Exception(f"Summary Agent produced invalid JSON. Raw output: {final_report_json_str}. Error: {e}")
            
            # Step 4: Generate Reading List
            yield {"event": "update", "data": json.dumps({'status': 'Analyzing', 'progress': 95, 'details': 'Generating reading list...'})}
            yield {"event": "checkpoint", "data": json.dumps({'text': 'Generating personalized reading list...'})}
            reading_list_agent = create_action_plan_agent() # Re-use the agent
            reading_list_prompt = f"""
            Task: Generate Reading List
            Context: Here is the full analysis of the BigQuery project. Please generate a reading list of 2-3 relevant articles or documentation pages that would be helpful for the user to read.
            {json.dumps(full_environment_data, indent=2)}
            """
            reading_list_json_str = await run_agent(reading_list_agent, reading_list_prompt)
            try:
                if reading_list_json_str.strip().startswith("```json"):
                    reading_list_json_str = reading_list_json_str.strip()[7:-4].strip()
                reading_list_obj = json.loads(reading_list_json_str)
            except json.JSONDecodeError as e:
                 # If reading list fails, we can proceed without it
                print(f"Agent produced invalid JSON for reading list. Raw output: {reading_list_json_str}. Error: {e}")
                reading_list_obj = {"reading_list": []}

            yield {"event": "checkpoint", "data": json.dumps({'text': 'Report generated successfully.'})}
            yield {"event": "update", "data": json.dumps({
                'status': 'Complete', 
                'progress': 100, 
                'report': final_report_obj,
                'reading_list': reading_list_obj.get('reading_list', [])
            })}

        except Exception as e:
            error_message = f"An error occurred during analysis: {e}"
            yield {"event": "error", "data": json.dumps({'status': 'Error', 'details': error_message})}

    return EventSourceResponse(event_stream())

def start():
    """Starts the Uvicorn server."""
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)

if __name__ == "__main__":
    start() 