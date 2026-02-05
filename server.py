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

## Field References

All field references use the format: ["field", FIELD_ID, OPTIONS]
- ["field", 5, null] - reference field ID 5
- ["field", 5, {"temporal-unit": "month"}] - field 5 grouped by month
- ["field", 5, {"join-alias": "Orders"}] - field from joined table

Use get_table_fields(table_id) to discover field IDs.

## Aggregations

Pass as list of aggregation clauses:
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

## Workflow

1. get_table_fields(table_id) - discover field IDs and types
2. get_field_values(field_id) - see distinct values for filters
3. execute_mbql_query(...) - test your query
4. create_mbql_card(...) - save as a card
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

    MBQL queries are more portable across databases and integrate better with
    Metabase's features like drill-down, automatic joins, and visualizations.

    Args:
        name: Name of the card.
        database_id: ID of the database.
        source_table_id: ID of the source table.
        aggregations: List of aggregations in MBQL format.
            Examples:
            - [["count"]] - count all rows
            - [["sum", ["field", 5, null]]] - sum of field ID 5
            - [["avg", ["field", 5, null]]] - average of field ID 5
            - [["distinct", ["field", 5, null]]] - distinct count of field
            - [["min", ["field", 5, null]]], [["max", ["field", 5, null]]]
            - Multiple: [["count"], ["sum", ["field", 5, null]]]
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
            Examples:
            - {"graph.dimensions": ["CATEGORY"], "graph.metrics": ["count"]}
            - {"graph.show_values": true}
            - {"table.pivot_column": "CATEGORY", "table.cell_column": "count"}

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
