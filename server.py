#!/usr/bin/env python3
"""
Metabase FastMCP Server

A FastMCP server that provides tools to interact with Metabase databases,
execute queries, manage cards, and work with collections.
"""

import logging
import os
import sys
from enum import Enum
from typing import Any

import httpx
from dotenv import load_dotenv
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware.error_handling import ErrorHandlingMiddleware
from fastmcp.server.middleware.logging import LoggingMiddleware

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get Metabase configuration from environment variables
METABASE_URL = os.getenv("METABASE_URL")
METABASE_USER_EMAIL = os.getenv("METABASE_USER_EMAIL")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD")
METABASE_API_KEY = os.getenv("METABASE_API_KEY")

if not METABASE_URL or (
    not METABASE_API_KEY and (not METABASE_USER_EMAIL or not METABASE_PASSWORD)
):
    raise ValueError(
        "METABASE_URL is required, and either METABASE_API_KEY or both METABASE_USER_EMAIL and METABASE_PASSWORD must be provided"
    )


# Authentication method enum
class AuthMethod(Enum):
    SESSION = "session"
    API_KEY = "api_key"


# Initialize FastMCP server with best practices configuration
mcp = FastMCP(
    name="metabase-mcp",
    on_duplicate_tools="error",  # Prevent accidental tool overwrites
    on_duplicate_resources="warn",  # Warn on resource conflicts
    on_duplicate_prompts="warn",  # Warn on prompt conflicts
)

# Add middleware for enhanced error handling and logging
mcp.add_middleware(ErrorHandlingMiddleware())  # Handle errors first
mcp.add_middleware(LoggingMiddleware())  # Log all operations


class MetabaseClient:
    """HTTP client for Metabase API operations"""

    def __init__(self):
        self.base_url = METABASE_URL.rstrip("/")
        self.session_token: str | None = None
        self.api_key: str | None = METABASE_API_KEY
        self.auth_method = AuthMethod.API_KEY if METABASE_API_KEY else AuthMethod.SESSION
        self.client = httpx.AsyncClient(timeout=30.0)

        logger.info(f"Using {self.auth_method.value} authentication method")

    async def _get_headers(self) -> dict[str, str]:
        """Get appropriate authentication headers"""
        headers = {"Content-Type": "application/json"}

        if self.auth_method == AuthMethod.API_KEY and self.api_key:
            headers["X-API-KEY"] = self.api_key
        elif self.auth_method == AuthMethod.SESSION:
            if not self.session_token:
                await self._get_session_token()
            if self.session_token:
                headers["X-Metabase-Session"] = self.session_token

        return headers

    async def _get_session_token(self) -> str:
        """Get Metabase session token for email/password authentication"""
        if self.auth_method == AuthMethod.API_KEY and self.api_key:
            return self.api_key

        if not METABASE_USER_EMAIL or not METABASE_PASSWORD:
            raise ValueError("Email and password required for session authentication")

        login_data = {"username": METABASE_USER_EMAIL, "password": METABASE_PASSWORD}

        response = await self.client.post(f"{self.base_url}/api/session", json=login_data)

        if response.status_code != 200:
            error_data = response.json() if response.content else {}
            raise Exception(f"Authentication failed: {response.status_code} - {error_data}")

        session_data = response.json()
        self.session_token = session_data.get("id")
        logger.info("Successfully obtained session token")
        return self.session_token

    async def request(self, method: str, path: str, **kwargs) -> dict[str, Any]:
        """Make authenticated request to Metabase API"""
        url = f"{self.base_url}/api{path}"
        headers = await self._get_headers()

        logger.debug(f"Making {method} request to {path}")

        response = await self.client.request(method=method, url=url, headers=headers, **kwargs)

        if not response.is_success:
            error_data = response.json() if response.content else {}
            error_message = (
                f"API request failed with status {response.status_code}: {response.text}"
            )
            logger.warning(f"{error_message} - {error_data}")
            raise Exception(error_message)

        logger.debug(f"Successful response from {path}")
        return response.json()

    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()


# Global client instance
metabase_client = MetabaseClient()


# =============================================================================
# Tool Definitions - Database Operations
# =============================================================================

@mcp.tool
async def list_databases(ctx: Context) -> dict[str, Any]:
    """
    List all databases configured in Metabase.

    Returns:
        A dictionary containing all available databases with their metadata.
    """
    try:
        await ctx.info("Fetching list of databases from Metabase")
        result = await metabase_client.request("GET", "/database")
        await ctx.info(f"Successfully retrieved {len(result.get('data', []))} databases")
        return result
    except Exception as e:
        error_msg = f"Error listing databases: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def list_tables(database_id: int, ctx: Context) -> str:
    """
    List all tables in a specific database.

    Args:
        database_id: The ID of the database to query.

    Returns:
        Formatted markdown table showing table details.
    """
    try:
        await ctx.info(f"Fetching tables for database {database_id}")
        result = await metabase_client.request("GET", f"/database/{database_id}/metadata")

        # Extract and format tables
        tables = result.get("tables", [])
        await ctx.debug(f"Found {len(tables)} tables in database {database_id}")

        formatted_tables = [
            {
                "table_id": table.get("id"),
                "display_name": table.get("display_name"),
                "description": table.get("description") or "No description",
                "entity_type": table.get("entity_type")
            }
            for table in tables
        ]

        # Sort for better readability
        formatted_tables.sort(key=lambda x: x.get("display_name", ""))

        # Generate markdown output
        markdown_output = f"# Tables in Database {database_id}\n\n"
        markdown_output += f"**Total Tables:** {len(formatted_tables)}\n\n"

        if not formatted_tables:
            await ctx.warning(f"No tables found in database {database_id}")
            markdown_output += "*No tables found in this database.*\n"
            return markdown_output

        # Create markdown table
        markdown_output += "| Table ID | Display Name | Description | Entity Type |\n"
        markdown_output += "|----------|--------------|-------------|--------------|\n"

        for table in formatted_tables:
            table_id = table.get("table_id", "N/A")
            display_name = table.get("display_name", "N/A")
            description = table.get("description", "No description")
            entity_type = table.get("entity_type", "N/A")

            # Escape pipe characters
            description = description.replace("|", "\\|")
            display_name = display_name.replace("|", "\\|")

            markdown_output += f"| {table_id} | {display_name} | {description} | {entity_type} |\n"

        await ctx.info(f"Successfully formatted {len(formatted_tables)} tables")
        return markdown_output

    except Exception as e:
        error_msg = f"Error listing tables for database {database_id}: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def get_table_fields(table_id: int, ctx: Context, limit: int = 20) -> dict[str, Any]:
    """
    Get all fields/columns in a specific table.

    Args:
        table_id: The ID of the table.
        limit: Maximum number of fields to return (default: 20).

    Returns:
        Dictionary with field metadata, truncated if necessary.
    """
    try:
        await ctx.info(f"Fetching fields for table {table_id}")
        result = await metabase_client.request("GET", f"/table/{table_id}/query_metadata")

        # Apply field limiting
        if limit > 0 and "fields" in result and len(result["fields"]) > limit:
            total_fields = len(result["fields"])
            result["fields"] = result["fields"][:limit]
            result["_truncated"] = True
            result["_total_fields"] = total_fields
            result["_limit_applied"] = limit
            await ctx.info(f"Truncated {total_fields} fields to {limit} fields")
        else:
            await ctx.info(f"Retrieved {len(result.get('fields', []))} fields")

        return result
    except Exception as e:
        error_msg = f"Error getting table fields for table {table_id}: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def get_field_values(field_id: int, ctx: Context) -> dict[str, Any]:
    """
    Get distinct values for a specific field. Useful for building MBQL filters.

    Args:
        field_id: The ID of the field to get values for.

    Returns:
        Dictionary containing the field's distinct values and metadata.
    """
    try:
        await ctx.info(f"Fetching values for field {field_id}")
        result = await metabase_client.request("GET", f"/field/{field_id}/values")

        value_count = len(result.get("values", []))
        await ctx.info(f"Retrieved {value_count} distinct values for field {field_id}")

        return result
    except Exception as e:
        error_msg = f"Error getting field values for field {field_id}: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


# =============================================================================
# Tool Definitions - MBQL Reference
# =============================================================================

MBQL_REFERENCE = """
# MBQL (Metabase Query Language) Reference

MBQL is Metabase's structured query language. It's database-agnostic and integrates
with Metabase features like drill-down, joins, and visualizations.

## ⚠️ METRICS-FIRST PHILOSOPHY

IMPORTANT: Before creating any aggregations, ALWAYS:
1. Call search_metrics_for_table(table_id) to find existing metrics
2. If a metric exists, use it with ["metric", metric_id] or create_card_with_metrics()
3. If no metric exists, ASK THE USER if they want to create a reusable metric first
4. Only use direct aggregations for one-off exploratory queries

This ensures calculations like "MQLs", "Revenue", "Active Users" are standardized.

## Using Metrics (PREFERRED)

Reference existing metrics in aggregations:
- [["metric", 5]] - use metric ID 5
- [["metric", 5], ["metric", 6]] - multiple metrics

Use create_card_with_metrics() for best results with proper naming.

## Field References

All field references use the format: ["field", FIELD_ID, OPTIONS]
- ["field", 5, null] - reference field ID 5
- ["field", 5, {"temporal-unit": "month"}] - field 5 grouped by month
- ["field", 5, {"join-alias": "Orders"}] - field from joined table

Use get_table_fields(table_id) to discover field IDs.

## Aggregations (use metrics when possible)

Pass as list of aggregation clauses:
- [["metric", 5]] - USE EXISTING METRIC (preferred)
- [["count"]] - count all rows
- [["sum", ["field", 5, null]]] - sum of field
- [["avg", ["field", 5, null]]] - average
- [["min", ["field", 5, null]]] - minimum
- [["max", ["field", 5, null]]] - maximum
- [["distinct", ["field", 5, null]]] - distinct count
- [["count"], ["sum", ["field", 5, null]]] - multiple aggregations

## Breakouts (Group By)

Pass as list of field references:
- [["field", 12, null]] - group by field
- [["field", 10, {"temporal-unit": "day"}]] - by day
- [["field", 10, {"temporal-unit": "week"}]] - by week
- [["field", 10, {"temporal-unit": "month"}]] - by month
- [["field", 10, {"temporal-unit": "quarter"}]] - by quarter
- [["field", 10, {"temporal-unit": "year"}]] - by year
- [["field", 10, {"binning": {"strategy": "num-bins", "num-bins": 10}}]] - numeric binning

## Filters

Single filter or combined with and/or:
- ["=", ["field", 5, null], "value"] - equals
- ["!=", ["field", 5, null], "value"] - not equals
- [">", ["field", 5, null], 100] - greater than
- [">=", ["field", 5, null], 100] - greater or equal
- ["<", ["field", 5, null], 100] - less than
- ["<=", ["field", 5, null], 100] - less or equal
- ["between", ["field", 5, null], 1, 100] - between (inclusive)
- ["contains", ["field", 5, null], "text"] - contains substring
- ["does-not-contain", ["field", 5, null], "text"] - excludes substring
- ["starts-with", ["field", 5, null], "prefix"] - starts with
- ["ends-with", ["field", 5, null], "suffix"] - ends with
- ["is-null", ["field", 5, null]] - is null
- ["not-null", ["field", 5, null]] - is not null
- ["is-empty", ["field", 5, null]] - is empty string
- ["not-empty", ["field", 5, null]] - is not empty
- ["and", FILTER1, FILTER2, ...] - all conditions must match
- ["or", FILTER1, FILTER2, ...] - any condition matches
- ["not", FILTER] - negate a filter

Date-specific filters:
- ["time-interval", ["field", 5, null], -30, "day"] - last 30 days
- ["time-interval", ["field", 5, null], "current", "month"] - current month

## Order By

Pass as list of order clauses:
- [["asc", ["field", 5, null]]] - ascending by field
- [["desc", ["field", 5, null]]] - descending by field
- [["desc", ["aggregation", 0]]] - by first aggregation result
- [["asc", ["aggregation", 1]]] - by second aggregation result

## Expressions (Calculated Columns)

Define computed columns as a dict:
- {"profit": ["-", ["field", 10, null], ["field", 11, null]]}
- {"full_name": ["concat", ["field", 1, null], " ", ["field", 2, null]]}
- {"tax": ["*", ["field", 5, null], 0.1]}

Reference in breakouts/order_by: ["expression", "profit"]

Math operators: +, -, *, /
String: concat, substring, trim, lower, upper, length
Date: datetime-add, datetime-subtract, get-year, get-month, get-day

## Joins

Join other tables:
```json
[{
    "source-table": TABLE_ID,
    "alias": "JoinAlias",
    "condition": ["=",
        ["field", LOCAL_FK_ID, null],
        ["field", REMOTE_PK_ID, {"join-alias": "JoinAlias"}]
    ],
    "fields": "all"  // or "none" or list of field refs
}]
```

## Display Types

table, bar, line, area, row, pie, scalar, progress, gauge, funnel,
scatter, waterfall, combo, map, pivot

## Column Naming

ALWAYS name your aggregation columns clearly using visualization_settings:
{
    "column_settings": {
        '["aggregation",0]': {"column_title": "MQLs"},
        '["aggregation",1]': {"column_title": "Revenue"}
    }
}

If unsure what to name a column, ASK THE USER.

## Recommended Workflow (Metrics-First)

1. get_table_fields(table_id) - discover field IDs and types
2. search_metrics_for_table(table_id) - CHECK FOR EXISTING METRICS FIRST
3. If metric exists:
   - create_card_with_metrics(...) - use the metric with proper naming
4. If no metric exists:
   - ASK USER: "Should I create a reusable metric for [calculation]?"
   - If yes: create_metric(...) - standardize the calculation
   - Then: create_card_with_metrics(...) - use the new metric
5. Only for exploratory/one-off queries:
   - execute_mbql_query(...) - test your query
   - create_mbql_card(...) - save as a card (with clear column names)
"""


@mcp.tool
async def get_mbql_reference(ctx: Context) -> str:
    """
    Get the MBQL (Metabase Query Language) reference documentation.

    Call this tool to understand how to construct MBQL queries for
    execute_mbql_query and create_mbql_card tools.

    Returns:
        Complete MBQL syntax reference with examples for aggregations,
        filters, breakouts, expressions, joins, and more.
    """
    await ctx.info("Returning MBQL reference documentation")
    return MBQL_REFERENCE


# =============================================================================
# Tool Definitions - Query Operations
# =============================================================================

@mcp.tool
async def execute_query(
    database_id: int,
    query: str,
    ctx: Context,
    native_parameters: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    """
    Execute a native SQL query against a Metabase database.

    Args:
        database_id: The ID of the database to query.
        query: The SQL query to execute.
        native_parameters: Optional parameters for the query.

    Returns:
        Query execution results.
    """
    try:
        await ctx.info(f"Executing query on database {database_id}")
        await ctx.debug(f"Query: {query[:100]}...")  # Log first 100 chars

        payload = {
            "database": database_id,
            "type": "native",
            "native": {"query": query}
        }

        if native_parameters:
            payload["native"]["parameters"] = native_parameters
            await ctx.debug(f"Query parameters: {len(native_parameters)} parameters provided")

        result = await metabase_client.request("POST", "/dataset", json=payload)

        row_count = len(result.get("data", {}).get("rows", []))
        await ctx.info(f"Query executed successfully, returned {row_count} rows")

        return result
    except Exception as e:
        error_msg = f"Error executing query: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def execute_mbql_query(
    database_id: int,
    source_table_id: int,
    ctx: Context,
    aggregations: list[list[Any]] | None = None,
    breakouts: list[list[Any]] | None = None,
    filters: list[Any] | None = None,
    order_by: list[list[Any]] | None = None,
    limit: int | None = None,
    expressions: dict[str, list[Any]] | None = None,
    joins: list[dict[str, Any]] | None = None,
    fields: list[list[Any]] | None = None,
) -> dict[str, Any]:
    """
    Execute an MBQL query against a Metabase database without saving it.
    Useful for testing queries before creating cards.

    Args:
        database_id: The ID of the database to query.
        source_table_id: The ID of the source table.
        aggregations: List of aggregations in MBQL format.
            Examples:
            - [["count"]] - count all rows
            - [["sum", ["field", 5, null]]] - sum of field ID 5
            - [["avg", ["field", 5, null]]] - average of field ID 5
            - [["distinct", ["field", 5, null]]] - distinct count
            - [["min", ["field", 5, null]]], [["max", ["field", 5, null]]]
        breakouts: List of fields to group by in MBQL format.
            Examples:
            - [["field", 12, null]] - group by field ID 12
            - [["field", 10, {"temporal-unit": "month"}]] - group by month
        filters: Filter conditions in MBQL format.
            Examples:
            - ["=", ["field", 5, null], "value"] - equals
            - [">", ["field", 5, null], 100] - greater than
            - ["between", ["field", 5, null], 1, 10] - between
            - ["contains", ["field", 5, null], "text"] - contains
            - ["and", [...], [...]] - combine filters
        order_by: Ordering specification.
            Examples:
            - [["asc", ["field", 5, null]]] - ascending by field
            - [["desc", ["aggregation", 0]]] - descending by first aggregation
        limit: Maximum rows to return.
        expressions: Custom calculated expressions.
            Example: {"profit": ["-", ["field", 10, null], ["field", 11, null]]}
        joins: Join specifications for multi-table queries.
        fields: Specific fields to select (if no aggregations).
            Example: [["field", 1, null], ["field", 2, null]]

    Returns:
        Query execution results.
    """
    try:
        await ctx.info(f"Executing MBQL query on database {database_id}, table {source_table_id}")

        # Build the MBQL query
        mbql_query: dict[str, Any] = {"source-table": source_table_id}

        if aggregations:
            mbql_query["aggregation"] = aggregations
            await ctx.debug(f"Aggregations: {len(aggregations)}")

        if breakouts:
            mbql_query["breakout"] = breakouts
            await ctx.debug(f"Breakouts: {len(breakouts)}")

        if filters:
            mbql_query["filter"] = filters
            await ctx.debug("Filter applied")

        if order_by:
            mbql_query["order-by"] = order_by
            await ctx.debug(f"Order by: {len(order_by)} clauses")

        if limit:
            mbql_query["limit"] = limit
            await ctx.debug(f"Limit: {limit}")

        if expressions:
            mbql_query["expressions"] = expressions
            await ctx.debug(f"Expressions: {len(expressions)}")

        if joins:
            mbql_query["joins"] = joins
            await ctx.debug(f"Joins: {len(joins)}")

        if fields:
            mbql_query["fields"] = fields
            await ctx.debug(f"Fields: {len(fields)}")

        payload = {
            "database": database_id,
            "type": "query",
            "query": mbql_query
        }

        result = await metabase_client.request("POST", "/dataset", json=payload)

        row_count = len(result.get("data", {}).get("rows", []))
        await ctx.info(f"MBQL query executed successfully, returned {row_count} rows")

        return result
    except Exception as e:
        error_msg = f"Error executing MBQL query: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


# =============================================================================
# Tool Definitions - Card/Question Operations
# =============================================================================

@mcp.tool
async def list_cards(ctx: Context) -> dict[str, Any]:
    """
    List all saved questions/cards in Metabase.

    Returns:
        Dictionary containing all cards with their metadata.
    """
    try:
        await ctx.info("Fetching list of saved cards/questions")
        result = await metabase_client.request("GET", "/card")
        card_count = len(result) if isinstance(result, list) else len(result.get("data", []))
        await ctx.info(f"Successfully retrieved {card_count} cards")
        return result
    except Exception as e:
        error_msg = f"Error listing cards: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def execute_card(
    card_id: int,
    ctx: Context,
    parameters: dict[str, Any] | None = None
) -> dict[str, Any]:
    """
    Execute a saved Metabase question/card and retrieve results.

    Args:
        card_id: The ID of the card to execute.
        parameters: Optional parameters for the card execution.

    Returns:
        Card execution results.
    """
    try:
        await ctx.info(f"Executing card {card_id}")
        payload = {}
        if parameters:
            payload["parameters"] = parameters
            await ctx.debug(f"Card parameters: {parameters}")

        result = await metabase_client.request("POST", f"/card/{card_id}/query", json=payload)

        row_count = len(result.get("data", {}).get("rows", []))
        await ctx.info(f"Card {card_id} executed successfully, returned {row_count} rows")

        return result
    except Exception as e:
        error_msg = f"Error executing card {card_id}: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def create_card(
    name: str,
    database_id: int,
    query: str,
    ctx: Context,
    description: str | None = None,
    collection_id: int | None = None,
    visualization_settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Create a new question/card in Metabase using native SQL.

    Args:
        name: Name of the card.
        database_id: ID of the database to query.
        query: SQL query for the card.
        description: Optional description.
        collection_id: Optional collection to place the card in.
        visualization_settings: Optional visualization configuration.

    Returns:
        The created card object.
    """
    try:
        await ctx.info(f"Creating new card '{name}' in database {database_id}")

        payload = {
            "name": name,
            "database_id": database_id,
            "dataset_query": {
                "database": database_id,
                "type": "native",
                "native": {"query": query},
            },
            "display": "table",
            "visualization_settings": visualization_settings or {},
        }

        if description:
            payload["description"] = description
        if collection_id is not None:
            payload["collection_id"] = collection_id
            await ctx.debug(f"Card will be placed in collection {collection_id}")

        result = await metabase_client.request("POST", "/card", json=payload)
        await ctx.info(f"Successfully created card with ID {result.get('id')}")

        return result
    except Exception as e:
        error_msg = f"Error creating card: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def create_card_with_metrics(
    name: str,
    database_id: int,
    source_table_id: int,
    metrics: list[dict[str, Any]],
    ctx: Context,
    breakouts: list[list[Any]] | None = None,
    filters: list[Any] | None = None,
    order_by: list[list[Any]] | None = None,
    limit: int | None = None,
    description: str | None = None,
    collection_id: int | None = None,
    display: str = "table",
    visualization_settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Create a new card using existing metrics (RECOMMENDED over create_mbql_card).

    This is the preferred way to create cards as it leverages standardized metrics,
    ensuring consistent calculations across your organization.

    Args:
        name: Name of the card.
        database_id: ID of the database.
        source_table_id: ID of the source table.
        metrics: List of metrics to include. Each metric should have:
            - id: The metric ID (required)
            - name: Display name for this metric in the card (optional, uses metric name if not provided)
            Example: [{"id": 5, "name": "MQLs"}, {"id": 6, "name": "Total Revenue"}]
        breakouts: Fields to group by (see create_mbql_card for format).
        filters: Additional filters to apply (see create_mbql_card for format).
        order_by: Ordering specification (see create_mbql_card for format).
        limit: Maximum rows to return.
        description: Optional description for the card.
        collection_id: Optional collection ID.
        display: Visualization type (table, bar, line, etc.).
        visualization_settings: Optional visualization configuration.

    Returns:
        The created card object including its ID.

    Example - Create a card showing MQLs by month:
        # First find the metric
        metrics = await search_metrics_for_table(table_id=42)
        mql_metric = next(m for m in metrics if m["name"] == "MQLs")

        # Create the card
        card = await create_card_with_metrics(
            name="MQLs by Month",
            database_id=1,
            source_table_id=42,
            metrics=[{"id": mql_metric["id"], "name": "MQLs"}],
            breakouts=[["field", 100, {"temporal-unit": "month"}]],
            display="line"
        )
    """
    try:
        await ctx.info(f"Creating card '{name}' with {len(metrics)} metric(s)")

        # Build aggregations from metric references
        aggregations = []
        column_settings = {}

        for i, metric_ref in enumerate(metrics):
            metric_id = metric_ref.get("id")
            if not metric_id:
                raise ValueError(f"Metric at index {i} missing 'id' field")

            # Reference the metric in MBQL format
            aggregations.append(["metric", metric_id])

            # If a custom name is provided, add it to visualization settings
            custom_name = metric_ref.get("name")
            if custom_name:
                # Metabase uses column keys for naming
                column_key = f'["aggregation",{i}]'
                column_settings[column_key] = {"column_title": custom_name}

        # Build the MBQL query
        mbql_query: dict[str, Any] = {
            "source-table": source_table_id,
            "aggregation": aggregations,
        }

        if breakouts:
            mbql_query["breakout"] = breakouts
        if filters:
            mbql_query["filter"] = filters
        if order_by:
            mbql_query["order-by"] = order_by
        if limit:
            mbql_query["limit"] = limit

        # Build visualization settings with column names
        viz_settings = visualization_settings or {}
        if column_settings:
            viz_settings["column_settings"] = {
                **viz_settings.get("column_settings", {}),
                **column_settings
            }

        payload: dict[str, Any] = {
            "name": name,
            "dataset_query": {
                "database": database_id,
                "type": "query",
                "query": mbql_query,
            },
            "display": display,
            "visualization_settings": viz_settings,
        }

        if description:
            payload["description"] = description
        if collection_id is not None:
            payload["collection_id"] = collection_id

        result = await metabase_client.request("POST", "/card", json=payload)
        await ctx.info(f"Successfully created card with ID {result.get('id')}")

        return result
    except Exception as e:
        error_msg = f"Error creating card with metrics: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def create_mbql_card(
    name: str,
    database_id: int,
    source_table_id: int,
    ctx: Context,
    aggregations: list[list[Any]] | None = None,
    breakouts: list[list[Any]] | None = None,
    filters: list[Any] | None = None,
    order_by: list[list[Any]] | None = None,
    limit: int | None = None,
    expressions: dict[str, list[Any]] | None = None,
    joins: list[dict[str, Any]] | None = None,
    fields: list[list[Any]] | None = None,
    description: str | None = None,
    collection_id: int | None = None,
    display: str = "table",
    visualization_settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Create a new question/card in Metabase using MBQL (Metabase Query Language).

    ⚠️  METRICS-FIRST PHILOSOPHY: Before using this tool with aggregations, you MUST:
        1. Call search_metrics_for_table(table_id) to check for existing metrics
        2. If a suitable metric exists, use create_card_with_metrics() instead
        3. If no metric exists, ASK THE USER if they want to create a reusable metric
        4. Only use direct aggregations here for one-off exploratory queries

    This ensures calculations are standardized across your organization.

    NAMING CLARITY: When using aggregations, always specify clear column names in
    visualization_settings.column_settings to avoid ambiguous names like "count".
    Example: {"column_settings": {'["aggregation",0]': {"column_title": "Total Orders"}}}

    If you're unsure what to name a column, ASK THE USER what they want it called.

    Args:
        name: Name of the card.
        database_id: ID of the database.
        source_table_id: ID of the source table.
        aggregations: List of aggregations in MBQL format.
            ⚠️  PREFER METRICS: Use ["metric", metric_id] to reference existing metrics.
            Only use direct aggregations for exploratory queries.
            Examples:
            - [["metric", 5]] - use metric ID 5 (PREFERRED)
            - [["count"]] - count all rows (use only if no metric exists)
            - [["sum", ["field", 5, null]]] - sum of field ID 5
            - [["avg", ["field", 5, null]]] - average of field ID 5
        breakouts: List of fields to group by in MBQL format.
            Examples:
            - [["field", 12, null]] - group by field ID 12
            - [["field", 10, {"temporal-unit": "month"}]] - group by month
            - [["field", 10, {"temporal-unit": "year"}]] - group by year
            - [["field", 10, {"binning": {"strategy": "num-bins", "num-bins": 10}}]]
        filters: Filter conditions in MBQL format.
            Examples:
            - ["=", ["field", 5, null], "value"] - equals
            - ["!=", ["field", 5, null], "value"] - not equals
            - [">", ["field", 5, null], 100] - greater than
            - ["<", ["field", 5, null], 100] - less than
            - [">=", ["field", 5, null], 100] - greater than or equal
            - ["between", ["field", 5, null], 1, 10] - between values
            - ["contains", ["field", 5, null], "text"] - contains substring
            - ["starts-with", ["field", 5, null], "prefix"]
            - ["is-null", ["field", 5, null]] - is null
            - ["not-null", ["field", 5, null]] - is not null
            - ["and", ["=", ...], [">", ...]] - combine with AND
            - ["or", ["=", ...], ["=", ...]] - combine with OR
        order_by: Ordering specification.
            Examples:
            - [["asc", ["field", 5, null]]] - ascending by field
            - [["desc", ["field", 5, null]]] - descending by field
            - [["desc", ["aggregation", 0]]] - descending by first aggregation
        limit: Maximum rows to return.
        expressions: Custom calculated expressions (computed columns).
            Example: {"profit": ["-", ["field", 10, null], ["field", 11, null]]}
            Then reference in breakouts/order as ["expression", "profit"]
        joins: Join specifications for multi-table queries.
            Example: [{
                "source-table": 3,
                "alias": "Orders",
                "condition": ["=", ["field", 7, null], ["field", 20, {"join-alias": "Orders"}]],
                "fields": "all"
            }]
        fields: Specific fields to select when not using aggregations.
            Example: [["field", 1, null], ["field", 2, null]]
        description: Optional description for the card.
        collection_id: Optional collection ID to place the card in.
        display: Visualization type. Options: table, bar, line, area, row, pie,
            scalar, progress, gauge, funnel, scatter, waterfall, combo, map.
        visualization_settings: Optional visualization configuration.
            IMPORTANT: Use column_settings to name aggregation columns clearly.
            Examples:
            - {"column_settings": {'["aggregation",0]': {"column_title": "MQLs"}}}
            - {"graph.dimensions": ["CATEGORY"], "graph.metrics": ["count"]}
            - {"graph.show_values": true}

    Returns:
        The created card object including its ID.
    """
    try:
        await ctx.info(f"Creating MBQL card '{name}' from table {source_table_id}")

        # Build the MBQL query
        mbql_query: dict[str, Any] = {"source-table": source_table_id}

        if aggregations:
            mbql_query["aggregation"] = aggregations
            await ctx.debug(f"Aggregations: {len(aggregations)}")

        if breakouts:
            mbql_query["breakout"] = breakouts
            await ctx.debug(f"Breakouts: {len(breakouts)}")

        if filters:
            mbql_query["filter"] = filters
            await ctx.debug("Filter applied")

        if order_by:
            mbql_query["order-by"] = order_by
            await ctx.debug(f"Order by: {len(order_by)} clauses")

        if limit:
            mbql_query["limit"] = limit
            await ctx.debug(f"Limit: {limit}")

        if expressions:
            mbql_query["expressions"] = expressions
            await ctx.debug(f"Expressions: {len(expressions)}")

        if joins:
            mbql_query["joins"] = joins
            await ctx.debug(f"Joins: {len(joins)}")

        if fields:
            mbql_query["fields"] = fields
            await ctx.debug(f"Fields: {len(fields)}")

        payload: dict[str, Any] = {
            "name": name,
            "database_id": database_id,
            "dataset_query": {
                "database": database_id,
                "type": "query",
                "query": mbql_query,
            },
            "display": display,
            "visualization_settings": visualization_settings or {},
        }

        if description:
            payload["description"] = description
        if collection_id is not None:
            payload["collection_id"] = collection_id
            await ctx.debug(f"Card will be placed in collection {collection_id}")

        result = await metabase_client.request("POST", "/card", json=payload)
        await ctx.info(f"Successfully created MBQL card with ID {result.get('id')}")

        return result
    except Exception as e:
        error_msg = f"Error creating MBQL card: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


# =============================================================================
# Tool Definitions - Collection Operations
# =============================================================================

@mcp.tool
async def list_collections(ctx: Context) -> dict[str, Any]:
    """
    List all collections in Metabase.

    Returns:
        Dictionary containing all collections with their metadata.
    """
    try:
        await ctx.info("Fetching list of collections")
        result = await metabase_client.request("GET", "/collection")
        collection_count = len(result) if isinstance(result, list) else len(result.get("data", []))
        await ctx.info(f"Successfully retrieved {collection_count} collections")
        return result
    except Exception as e:
        error_msg = f"Error listing collections: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def create_collection(
    name: str,
    ctx: Context,
    description: str | None = None,
    color: str | None = None,
    parent_id: int | None = None,
) -> dict[str, Any]:
    """
    Create a new collection in Metabase.

    Args:
        name: Name of the collection.
        description: Optional description.
        color: Optional color for the collection.
        parent_id: Optional parent collection ID.

    Returns:
        The created collection object.
    """
    try:
        await ctx.info(f"Creating new collection '{name}'")

        payload = {"name": name}

        if description:
            payload["description"] = description
        if color:
            payload["color"] = color
            await ctx.debug(f"Collection color: {color}")
        if parent_id is not None:
            payload["parent_id"] = parent_id
            await ctx.debug(f"Collection parent ID: {parent_id}")

        result = await metabase_client.request("POST", "/collection", json=payload)
        await ctx.info(f"Successfully created collection with ID {result.get('id')}")

        return result
    except Exception as e:
        error_msg = f"Error creating collection: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


# =============================================================================
# Tool Definitions - Dashboard Operations
# =============================================================================

@mcp.tool
async def list_dashboards(ctx: Context) -> dict[str, Any]:
    """
    List all dashboards in Metabase.

    Returns:
        Dictionary containing all dashboards with their metadata.
    """
    try:
        await ctx.info("Fetching list of dashboards")
        result = await metabase_client.request("GET", "/dashboard")
        dashboard_count = len(result) if isinstance(result, list) else len(result.get("data", []))
        await ctx.info(f"Successfully retrieved {dashboard_count} dashboards")
        return result
    except Exception as e:
        error_msg = f"Error listing dashboards: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def get_dashboard(dashboard_id: int, ctx: Context) -> dict[str, Any]:
    """
    Get full dashboard details including cards and parameters.

    Args:
        dashboard_id: ID of the dashboard.

    Returns:
        Complete dashboard object including dashcards and parameters.
    """
    try:
        await ctx.info(f"Fetching dashboard {dashboard_id}")
        result = await metabase_client.request("GET", f"/dashboard/{dashboard_id}")

        dashcard_count = len(result.get("dashcards", result.get("ordered_cards", [])))
        param_count = len(result.get("parameters", []))
        await ctx.info(f"Dashboard has {dashcard_count} cards and {param_count} parameters")

        return result
    except Exception as e:
        error_msg = f"Error getting dashboard {dashboard_id}: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def create_dashboard(
    name: str,
    ctx: Context,
    description: str | None = None,
    collection_id: int | None = None,
    parameters: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Create a new dashboard in Metabase.

    Args:
        name: Name of the dashboard.
        description: Optional description.
        collection_id: Optional collection ID to place the dashboard in.
        parameters: Optional list of dashboard filter parameters.
            Example parameter:
            {
                "id": "category_filter",
                "name": "Category",
                "slug": "category",
                "type": "string/=",
                "sectionId": "string"
            }

    Returns:
        The created dashboard object including its ID.
    """
    try:
        await ctx.info(f"Creating new dashboard '{name}'")

        payload: dict[str, Any] = {"name": name}

        if description:
            payload["description"] = description
        if collection_id is not None:
            payload["collection_id"] = collection_id
            await ctx.debug(f"Dashboard will be placed in collection {collection_id}")
        if parameters:
            payload["parameters"] = parameters
            await ctx.debug(f"Dashboard will have {len(parameters)} parameters")

        result = await metabase_client.request("POST", "/dashboard", json=payload)
        await ctx.info(f"Successfully created dashboard with ID {result.get('id')}")

        return result
    except Exception as e:
        error_msg = f"Error creating dashboard: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def update_dashboard(
    dashboard_id: int,
    ctx: Context,
    name: str | None = None,
    description: str | None = None,
    collection_id: int | None = None,
    parameters: list[dict[str, Any]] | None = None,
    archived: bool | None = None,
) -> dict[str, Any]:
    """
    Update an existing dashboard in Metabase.

    Args:
        dashboard_id: ID of the dashboard to update.
        name: New name for the dashboard.
        description: New description.
        collection_id: New collection ID to move the dashboard to.
        parameters: Updated list of dashboard filter parameters.
        archived: Set to true to archive the dashboard.

    Returns:
        The updated dashboard object.
    """
    try:
        await ctx.info(f"Updating dashboard {dashboard_id}")

        payload: dict[str, Any] = {}

        if name is not None:
            payload["name"] = name
        if description is not None:
            payload["description"] = description
        if collection_id is not None:
            payload["collection_id"] = collection_id
        if parameters is not None:
            payload["parameters"] = parameters
            await ctx.debug(f"Updating dashboard with {len(parameters)} parameters")
        if archived is not None:
            payload["archived"] = archived
            await ctx.debug(f"Setting archived: {archived}")

        if not payload:
            raise ValueError("No update fields provided")

        result = await metabase_client.request("PUT", f"/dashboard/{dashboard_id}", json=payload)
        await ctx.info(f"Successfully updated dashboard {dashboard_id}")

        return result
    except Exception as e:
        error_msg = f"Error updating dashboard {dashboard_id}: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def add_card_to_dashboard(
    dashboard_id: int,
    card_id: int,
    ctx: Context,
    size_x: int = 4,
    size_y: int = 3,
    row: int = 0,
    col: int = 0,
    parameter_mappings: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Add a card/question to a dashboard.

    Note: Since Metabase 0.47+, this uses PUT /dashboard/:id with dashcards array.
    New cards use negative IDs to signal creation.

    Args:
        dashboard_id: ID of the dashboard.
        card_id: ID of the card/question to add.
        size_x: Width of the card in grid units (default: 4).
        size_y: Height of the card in grid units (default: 3).
        row: Row position in the dashboard grid (default: 0).
        col: Column position in the dashboard grid (default: 0).
        parameter_mappings: Optional mappings connecting dashboard filters to card variables.
            Example:
            [
                {
                    "parameter_id": "category_filter",
                    "card_id": 123,
                    "target": ["variable", ["template-tag", "category"]]
                }
            ]

    Returns:
        The updated dashboard object.
    """
    try:
        await ctx.info(f"Adding card {card_id} to dashboard {dashboard_id}")

        # First get existing dashboard to preserve existing cards
        dashboard = await metabase_client.request("GET", f"/dashboard/{dashboard_id}")
        existing_dashcards = dashboard.get("dashcards", dashboard.get("ordered_cards", []))

        await ctx.debug(f"Dashboard currently has {len(existing_dashcards)} cards")

        # Add new card with negative ID (signals creation in Metabase 0.47+)
        new_dashcard: dict[str, Any] = {
            "id": -1,
            "card_id": card_id,
            "size_x": size_x,
            "size_y": size_y,
            "row": row,
            "col": col,
            "parameter_mappings": parameter_mappings or [],
        }

        result = await metabase_client.request(
            "PUT",
            f"/dashboard/{dashboard_id}",
            json={"dashcards": [*existing_dashcards, new_dashcard]}
        )

        await ctx.info(f"Successfully added card {card_id} to dashboard {dashboard_id}")
        return result
    except Exception as e:
        error_msg = f"Error adding card to dashboard: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def remove_card_from_dashboard(
    dashboard_id: int,
    dashcard_id: int,
    ctx: Context,
) -> dict[str, Any]:
    """
    Remove a card from a dashboard.

    Note: This requires the dashcard_id (the ID of the card placement on the dashboard),
    not the card_id. Use get_dashboard to find the dashcard_id.

    Args:
        dashboard_id: ID of the dashboard.
        dashcard_id: ID of the dashcard (card placement) to remove.

    Returns:
        The updated dashboard object.
    """
    try:
        await ctx.info(f"Removing dashcard {dashcard_id} from dashboard {dashboard_id}")

        # Get existing dashboard
        dashboard = await metabase_client.request("GET", f"/dashboard/{dashboard_id}")
        existing_dashcards = dashboard.get("dashcards", dashboard.get("ordered_cards", []))

        # Filter out the dashcard to remove
        filtered_dashcards = [dc for dc in existing_dashcards if dc.get("id") != dashcard_id]

        if len(filtered_dashcards) == len(existing_dashcards):
            raise ValueError(f"Dashcard {dashcard_id} not found on dashboard {dashboard_id}")

        result = await metabase_client.request(
            "PUT",
            f"/dashboard/{dashboard_id}",
            json={"dashcards": filtered_dashcards}
        )

        await ctx.info(f"Successfully removed dashcard {dashcard_id} from dashboard {dashboard_id}")
        return result
    except Exception as e:
        error_msg = f"Error removing card from dashboard: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def add_dashboard_filter(
    dashboard_id: int,
    parameters: list[dict[str, Any]],
    ctx: Context,
) -> dict[str, Any]:
    """
    Add or update filter parameters on a dashboard.

    This sets the dashboard's parameters array. To add new filters while keeping
    existing ones, first use get_dashboard to retrieve current parameters.

    Args:
        dashboard_id: ID of the dashboard.
        parameters: Array of dashboard filter parameters. Each parameter should have:
            - id: Unique parameter ID (string)
            - name: Display name for the filter
            - slug: URL slug for the parameter
            - type: Parameter type (e.g., "string/=", "number/=", "category", "date/single")
            - sectionId: Optional section ID ("string", "number", "date", "location", "id")
            - default: Optional default value
            - values_source_type: Optional, one of "static-list", "card", or null
            - values_source_config: Optional configuration for dropdown values
                For "static-list": {"values": [["value1", "Label 1"], ["value2", "Label 2"]]}
                For "card": {"card_id": 123, "value_field": [...], "label_field": [...]}

            Example parameters:
            [
                {
                    "id": "category_filter",
                    "name": "Category",
                    "slug": "category",
                    "type": "string/=",
                    "sectionId": "string"
                },
                {
                    "id": "date_filter",
                    "name": "Date Range",
                    "slug": "date",
                    "type": "date/range",
                    "sectionId": "date"
                }
            ]

    Returns:
        The updated dashboard object.
    """
    try:
        await ctx.info(f"Adding {len(parameters)} filter(s) to dashboard {dashboard_id}")

        result = await metabase_client.request(
            "PUT",
            f"/dashboard/{dashboard_id}",
            json={"parameters": parameters}
        )

        await ctx.info(f"Successfully updated filters on dashboard {dashboard_id}")
        return result
    except Exception as e:
        error_msg = f"Error adding filters to dashboard: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def update_dashboard_cards(
    dashboard_id: int,
    dashcards: list[dict[str, Any]],
    ctx: Context,
) -> dict[str, Any]:
    """
    Update dashboard cards including their positions and parameter mappings.

    Use this to reposition cards or connect dashboard filters to card variables.

    Args:
        dashboard_id: ID of the dashboard.
        dashcards: Array of dashcard configurations. Each dashcard should have:
            - id: Dashcard ID (use get_dashboard to find these)
            - card_id: Card/Question ID
            - row: Row position in grid
            - col: Column position in grid
            - size_x: Width in grid units
            - size_y: Height in grid units
            - parameter_mappings: Array connecting dashboard filters to card variables
                Example mapping:
                {
                    "parameter_id": "category_filter",
                    "card_id": 123,
                    "target": ["variable", ["template-tag", "category"]]
                }
                Or for dimension targets:
                {
                    "parameter_id": "date_filter",
                    "card_id": 123,
                    "target": ["dimension", ["field", 456, null]]
                }

    Returns:
        The updated dashboard object.
    """
    try:
        await ctx.info(f"Updating {len(dashcards)} cards on dashboard {dashboard_id}")

        result = await metabase_client.request(
            "PUT",
            f"/dashboard/{dashboard_id}",
            json={"dashcards": dashcards}
        )

        await ctx.info(f"Successfully updated cards on dashboard {dashboard_id}")
        return result
    except Exception as e:
        error_msg = f"Error updating dashboard cards: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


# =============================================================================
# Tool Definitions - Metric Operations
# =============================================================================

@mcp.tool
async def list_metrics(ctx: Context) -> dict[str, Any]:
    """
    List all metrics in Metabase.

    Metrics are reusable aggregation definitions that standardize how important
    numbers are calculated across your organization.

    IMPORTANT: Always check for existing metrics before creating new aggregations.
    Use search_metrics_for_table() to find metrics relevant to your query.

    Returns:
        List of metrics with their metadata.
    """
    try:
        await ctx.info("Fetching list of metrics")
        # Metrics are cards with type="metric"
        all_cards = await metabase_client.request("GET", "/card")
        if isinstance(all_cards, list):
            result = [c for c in all_cards if c.get("type") == "metric"]
        else:
            result = []

        metric_count = len(result)
        await ctx.info(f"Successfully retrieved {metric_count} metrics")
        return result
    except Exception as e:
        error_msg = f"Error listing metrics: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def search_metrics_for_table(
    table_id: int,
    ctx: Context,
    database_id: int | None = None,
) -> list[dict[str, Any]]:
    """
    Search for existing metrics that use a specific table as their source.

    IMPORTANT: Call this BEFORE creating any new cards or aggregations.
    This supports the metrics-first philosophy where we reuse standardized
    metrics rather than creating ad-hoc aggregations.

    Args:
        table_id: The table ID to search for metrics on.
        database_id: Optional database ID to narrow the search.

    Returns:
        List of metrics that use the specified table, including:
        - id: Metric ID (for use in create_card_with_metrics)
        - name: The metric name (e.g., "MQLs", "Revenue")
        - description: What the metric measures
        - aggregation: The aggregation type (count, sum, avg, etc.)

    Example workflow:
        1. search_metrics_for_table(table_id=42) -> Find existing metrics
        2. If metric exists: create_card_with_metrics(metrics=[{"id": 5, "name": "MQLs"}])
        3. If no metric exists: Ask user if they want to create one first
    """
    try:
        await ctx.info(f"Searching for metrics using table {table_id}")

        # Get all metrics
        all_cards = await metabase_client.request("GET", "/card")
        metrics = [c for c in all_cards if c.get("type") == "metric"] if isinstance(all_cards, list) else []

        # Filter to metrics that use this table
        matching_metrics = []
        for metric in metrics:
            dataset_query = metric.get("dataset_query", {})
            query = dataset_query.get("query", {})
            source_table = query.get("source-table")

            # Check if metric uses this table
            if source_table == table_id:
                # Also check database if specified
                if database_id and dataset_query.get("database") != database_id:
                    continue

                # Extract useful info about the metric
                aggregation = query.get("aggregation", [[]])[0] if query.get("aggregation") else None
                agg_type = aggregation[0] if aggregation else "unknown"

                matching_metrics.append({
                    "id": metric.get("id"),
                    "name": metric.get("name"),
                    "description": metric.get("description"),
                    "aggregation_type": agg_type,
                    "has_filter": "filter" in query,
                    "collection_id": metric.get("collection_id"),
                })

        await ctx.info(f"Found {len(matching_metrics)} metrics for table {table_id}")

        if not matching_metrics:
            await ctx.warning(
                f"No metrics found for table {table_id}. "
                "Consider creating a metric first to standardize this calculation."
            )

        return matching_metrics
    except Exception as e:
        error_msg = f"Error searching metrics for table {table_id}: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def get_metric(metric_id: int, ctx: Context) -> dict[str, Any]:
    """
    Get details of a specific metric.

    Args:
        metric_id: ID of the metric (same as card ID).

    Returns:
        Complete metric object including its definition.
    """
    try:
        await ctx.info(f"Fetching metric {metric_id}")
        result = await metabase_client.request("GET", f"/card/{metric_id}")

        if result.get("type") != "metric":
            await ctx.warning(f"Card {metric_id} is not a metric, it's a {result.get('type')}")

        return result
    except Exception as e:
        error_msg = f"Error getting metric {metric_id}: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def create_metric(
    name: str,
    database_id: int,
    source_table_id: int,
    aggregation: list[Any],
    ctx: Context,
    filters: list[Any] | None = None,
    description: str | None = None,
    collection_id: int | None = None,
    default_time_dimension_field_id: int | None = None,
    default_time_dimension_unit: str | None = None,
) -> dict[str, Any]:
    """
    Create a new metric in Metabase.

    Metrics are reusable aggregation definitions that standardize calculations.
    They're created as cards with type="metric".

    The metric name is automatically applied to the aggregation expression so it
    appears correctly when the metric is used in questions and dashboards.

    Args:
        name: Name of the metric (e.g., "MQLs", "Revenue", "Active Users").
            This name will appear as the column header when the metric is used.
        database_id: ID of the database.
        source_table_id: ID of the source table.
        aggregation: The aggregation formula in MBQL format.
            Examples:
            - ["count"] - count all rows
            - ["sum", ["field", 5, null]] - sum of field ID 5
            - ["avg", ["field", 5, null]] - average of field ID 5
            - ["distinct", ["field", 5, null]] - distinct count
        filters: Optional filter conditions in MBQL format.
            Examples:
            - ["=", ["field", 10, null], true] - where field 10 is true
            - ["=", ["field", 10, null], "active"] - where field 10 equals "active"
            - ["and", ["=", ...], [">", ...]] - combine with AND
        description: Optional description explaining what this metric measures.
        collection_id: Optional collection ID to place the metric in.
        default_time_dimension_field_id: Optional field ID for default time grouping.
        default_time_dimension_unit: Optional time unit for default grouping
            (e.g., "day", "week", "month", "quarter", "year").

    Returns:
        The created metric object including its ID.

    Example - Create MQLs metric (count where is_mql = true):
        create_metric(
            name="MQLs",
            database_id=1,
            source_table_id=42,  # gtm.leads table
            aggregation=["count"],
            filters=["=", ["field", 105, null], true],  # is_mql field
            description="Marketing Qualified Leads - count of leads where is_mql is true"
        )

    Example - Create Revenue metric (sum of amount):
        create_metric(
            name="Revenue",
            database_id=1,
            source_table_id=50,  # orders table
            aggregation=["sum", ["field", 200, null]],  # amount field
            description="Total revenue from all orders"
        )
    """
    try:
        await ctx.info(f"Creating metric '{name}' from table {source_table_id}")

        # Wrap the aggregation with aggregation-options to include the metric name
        # This ensures the name appears when the metric is used in questions
        named_aggregation = [
            "aggregation-options",
            aggregation,
            {"name": name, "display-name": name}
        ]

        # Build the MBQL query for the metric
        mbql_query: dict[str, Any] = {
            "source-table": source_table_id,
            "aggregation": [named_aggregation],
        }

        if filters:
            mbql_query["filter"] = filters
            await ctx.debug("Filter applied to metric")

        # Add default time dimension if specified
        if default_time_dimension_field_id:
            breakout_options: dict[str, Any] = {}
            if default_time_dimension_unit:
                breakout_options["temporal-unit"] = default_time_dimension_unit
            mbql_query["breakout"] = [
                ["field", default_time_dimension_field_id, breakout_options or None]
            ]
            await ctx.debug(f"Default time dimension set to field {default_time_dimension_field_id}")

        # Build the payload - metrics use the card API with type="metric"
        payload: dict[str, Any] = {
            "name": name,
            "type": "metric",
            "dataset_query": {
                "database": database_id,
                "type": "query",
                "query": mbql_query,
            },
            "display": "scalar" if not default_time_dimension_field_id else "line",
            "visualization_settings": {},
        }

        if description:
            payload["description"] = description
        if collection_id is not None:
            payload["collection_id"] = collection_id
            await ctx.debug(f"Metric will be placed in collection {collection_id}")

        result = await metabase_client.request("POST", "/card", json=payload)
        await ctx.info(f"Successfully created metric '{name}' with ID {result.get('id')}")

        return result
    except Exception as e:
        error_msg = f"Error creating metric: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def update_metric(
    metric_id: int,
    ctx: Context,
    name: str | None = None,
    description: str | None = None,
    aggregation: list[Any] | None = None,
    filters: list[Any] | None = None,
    collection_id: int | None = None,
    archived: bool | None = None,
) -> dict[str, Any]:
    """
    Update an existing metric in Metabase.

    Args:
        metric_id: ID of the metric to update.
        name: New name for the metric (also updates the aggregation display name).
        description: New description.
        aggregation: New aggregation formula in MBQL format.
        filters: New filter conditions in MBQL format.
        collection_id: New collection ID to move the metric to.
        archived: Set to true to archive the metric.

    Returns:
        The updated metric object.
    """
    try:
        await ctx.info(f"Updating metric {metric_id}")

        # Fetch current metric to get existing values
        current = await metabase_client.request("GET", f"/card/{metric_id}")
        current_name = current.get("name", "")

        payload: dict[str, Any] = {}

        # Determine the effective name (new name or current name)
        effective_name = name if name is not None else current_name

        if name is not None:
            payload["name"] = name
        if description is not None:
            payload["description"] = description
        if collection_id is not None:
            payload["collection_id"] = collection_id
        if archived is not None:
            payload["archived"] = archived

        # If updating the query definition or the name, we need to update the aggregation
        if aggregation is not None or filters is not None or name is not None:
            dataset_query = current.get("dataset_query", {})
            query = dataset_query.get("query", {})

            if aggregation is not None:
                # Wrap with aggregation-options to include the name
                named_aggregation = [
                    "aggregation-options",
                    aggregation,
                    {"name": effective_name, "display-name": effective_name}
                ]
                query["aggregation"] = [named_aggregation]
            elif name is not None:
                # If only name changed, update the existing aggregation's display name
                existing_agg = query.get("aggregation", [[]])[0]
                if existing_agg and existing_agg[0] == "aggregation-options":
                    # Already wrapped, update the options
                    existing_agg[2]["name"] = effective_name
                    existing_agg[2]["display-name"] = effective_name
                    query["aggregation"] = [existing_agg]
                elif existing_agg:
                    # Not wrapped yet, wrap it
                    named_aggregation = [
                        "aggregation-options",
                        existing_agg,
                        {"name": effective_name, "display-name": effective_name}
                    ]
                    query["aggregation"] = [named_aggregation]

            if filters is not None:
                if filters:
                    query["filter"] = filters
                else:
                    query.pop("filter", None)

            dataset_query["query"] = query
            payload["dataset_query"] = dataset_query
            await ctx.debug("Updated metric query definition")

        if not payload:
            raise ValueError("No update fields provided")

        result = await metabase_client.request("PUT", f"/card/{metric_id}", json=payload)
        await ctx.info(f"Successfully updated metric {metric_id}")

        return result
    except Exception as e:
        error_msg = f"Error updating metric {metric_id}: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


@mcp.tool
async def copy_dashboard_tab(
    source_dashboard_id: int,
    target_dashboard_id: int,
    tab_id: int,
    ctx: Context,
    new_tab_name: str | None = None,
    include_filters: bool = True,
) -> dict[str, Any]:
    """
    Copy a tab from one dashboard to another, including all cards and filters.

    This copies:
    - The tab itself (with optional rename)
    - All cards/dashcards on that tab (positions, sizes, visualization settings)
    - Parameter mappings connecting filters to cards
    - Dashboard parameters/filters used by cards on the tab (if include_filters=True)

    Args:
        source_dashboard_id: ID of the dashboard to copy from.
        target_dashboard_id: ID of the dashboard to copy to.
        tab_id: ID of the tab to copy (use get_dashboard to find tab IDs).
        new_tab_name: Optional new name for the tab (defaults to original name).
        include_filters: Whether to copy the dashboard filters used by this tab's cards.
            Set to False if the target dashboard already has the needed filters.

    Returns:
        The updated target dashboard object.

    Example:
        # First, inspect the source dashboard to find the tab ID
        source = await get_dashboard(dashboard_id=100)
        # source["tabs"] contains: [{"id": 1, "name": "Overview"}, {"id": 2, "name": "Details"}]

        # Copy the "Details" tab to another dashboard
        result = await copy_dashboard_tab(
            source_dashboard_id=100,
            target_dashboard_id=200,
            tab_id=2,
            new_tab_name="Copied Details"
        )
    """
    try:
        await ctx.info(f"Copying tab {tab_id} from dashboard {source_dashboard_id} to {target_dashboard_id}")

        # Fetch both dashboards
        source_dash = await metabase_client.request("GET", f"/dashboard/{source_dashboard_id}")
        target_dash = await metabase_client.request("GET", f"/dashboard/{target_dashboard_id}")

        # Find the source tab
        source_tabs = source_dash.get("tabs", [])
        source_tab = next((t for t in source_tabs if t.get("id") == tab_id), None)
        if not source_tab:
            raise ValueError(f"Tab {tab_id} not found in source dashboard. Available tabs: {source_tabs}")

        tab_name = new_tab_name or source_tab.get("name", "Copied Tab")
        await ctx.debug(f"Copying tab '{source_tab.get('name')}' as '{tab_name}'")

        # Get all dashcards on this tab
        source_dashcards = source_dash.get("dashcards", source_dash.get("ordered_cards", []))
        tab_dashcards = [dc for dc in source_dashcards if dc.get("dashboard_tab_id") == tab_id]
        await ctx.debug(f"Found {len(tab_dashcards)} cards on tab")

        # Get target dashboard's existing tabs and dashcards
        target_tabs = target_dash.get("tabs", [])
        target_dashcards = target_dash.get("dashcards", target_dash.get("ordered_cards", []))
        target_params = target_dash.get("parameters", [])

        # Create a new tab ID (negative signals creation)
        # Find the lowest negative ID to use
        existing_tab_ids = [t.get("id", 0) for t in target_tabs]
        new_tab_id = min(min(existing_tab_ids, default=0) - 1, -1)

        # Create the new tab
        new_tab = {
            "id": new_tab_id,
            "name": tab_name,
        }

        # Identify which parameters are used by cards on this tab
        used_param_ids = set()
        for dc in tab_dashcards:
            for mapping in dc.get("parameter_mappings", []):
                used_param_ids.add(mapping.get("parameter_id"))

        # Copy relevant parameters if requested
        params_to_add = []
        param_id_mapping = {}  # old_id -> new_id (in case of conflicts)

        if include_filters and used_param_ids:
            source_params = source_dash.get("parameters", [])
            existing_param_ids = {p.get("id") for p in target_params}

            for param in source_params:
                param_id = param.get("id")
                if param_id in used_param_ids:
                    if param_id in existing_param_ids:
                        # Parameter ID conflict - create a new ID
                        new_param_id = f"{param_id}_copy"
                        counter = 1
                        while new_param_id in existing_param_ids:
                            new_param_id = f"{param_id}_copy_{counter}"
                            counter += 1
                        param_id_mapping[param_id] = new_param_id
                        new_param = {**param, "id": new_param_id}
                        params_to_add.append(new_param)
                        await ctx.debug(f"Parameter '{param_id}' conflicts, using '{new_param_id}'")
                    else:
                        # No conflict, copy as-is
                        params_to_add.append(param.copy())

            await ctx.debug(f"Copying {len(params_to_add)} parameters")

        # Copy dashcards with new IDs and updated tab reference
        # Find the lowest existing dashcard ID to generate new negative IDs
        existing_dc_ids = [dc.get("id", 0) for dc in target_dashcards]
        next_dc_id = min(min(existing_dc_ids, default=0) - 1, -1)

        new_dashcards = []
        for dc in tab_dashcards:
            # Create a copy with new ID and tab reference
            new_dc = {
                "id": next_dc_id,
                "card_id": dc.get("card_id"),
                "dashboard_tab_id": new_tab_id,
                "row": dc.get("row", 0),
                "col": dc.get("col", 0),
                "size_x": dc.get("size_x", 4),
                "size_y": dc.get("size_y", 3),
                "visualization_settings": dc.get("visualization_settings", {}),
            }

            # Copy parameter mappings, updating IDs if needed
            param_mappings = []
            for mapping in dc.get("parameter_mappings", []):
                new_mapping = mapping.copy()
                old_param_id = mapping.get("parameter_id")
                if old_param_id in param_id_mapping:
                    new_mapping["parameter_id"] = param_id_mapping[old_param_id]
                # Update card_id in mapping to reference the same card
                new_mapping["card_id"] = dc.get("card_id")
                param_mappings.append(new_mapping)

            new_dc["parameter_mappings"] = param_mappings
            new_dashcards.append(new_dc)
            next_dc_id -= 1

        # Build the update payload
        updated_tabs = target_tabs + [new_tab]
        updated_dashcards = target_dashcards + new_dashcards
        updated_params = target_params + params_to_add

        payload: dict[str, Any] = {
            "tabs": updated_tabs,
            "dashcards": updated_dashcards,
        }

        if params_to_add:
            payload["parameters"] = updated_params

        # Update the target dashboard
        result = await metabase_client.request("PUT", f"/dashboard/{target_dashboard_id}", json=payload)

        await ctx.info(
            f"Successfully copied tab '{tab_name}' with {len(new_dashcards)} cards "
            f"and {len(params_to_add)} filters to dashboard {target_dashboard_id}"
        )

        return result
    except Exception as e:
        error_msg = f"Error copying dashboard tab: {e}"
        await ctx.error(error_msg)
        raise ToolError(error_msg) from e


def main():
    """
    Main entry point for the Metabase MCP server.

    Supports multiple transport methods:
    - STDIO (default): For IDE integration
    - SSE: Server-Sent Events for web apps
    - HTTP: Standard HTTP for API access
    """
    # Get configuration from environment
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))

    # Parse transport argument
    transport = "stdio"  # default
    if "--sse" in sys.argv:
        transport = "sse"
    elif "--http" in sys.argv:
        transport = "streamable-http"
    elif "--stdio" in sys.argv:
        transport = "stdio"

    logger.info(f"Starting Metabase MCP server with {transport} transport")

    try:
        # Run server with appropriate transport
        if transport in ["sse", "streamable-http"]:
            logger.info(f"Server will be available at http://{host}:{port}")
            mcp.run(transport=transport, host=host, port=port)
        else:
            mcp.run(transport=transport)

    except KeyboardInterrupt:
        logger.info("Server shutdown requested")
    except Exception as e:
        logger.error(f"Server error: {e}")
        raise


if __name__ == "__main__":
    main()
