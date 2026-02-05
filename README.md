# Metabase MCP Server - Connect AI Assistants to Your Metabase Analytics

[![PyPI version](https://badge.fury.io/py/metabase-mcp.svg)](https://badge.fury.io/py/metabase-mcp)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![FastMCP](https://img.shields.io/badge/FastMCP-v2.12+-green.svg)](https://github.com/jlowin/fastmcp)

A high-performance **Model Context Protocol (MCP) server** for **Metabase**, enabling AI assistants like **Claude**, **Cursor**, and other MCP clients to interact seamlessly with your Metabase instance. Query databases, execute SQL, manage dashboards, and automate analytics workflows with natural language through AI-powered database operations.

**Perfect for:** Data analysts, developers, and teams looking to integrate AI assistants with their Metabase business intelligence platform for automated SQL queries, dashboard management, and data exploration.

## Key Features

### Database Operations
- **List Databases**: Browse all configured Metabase databases
- **Table Discovery**: Explore tables with metadata and descriptions
- **Field Inspection**: Get detailed field/column information with smart pagination

### Query & Analytics
- **SQL Execution**: Run native SQL queries with parameter support and templating
- **Card Management**: Execute, create, and manage Metabase questions/cards
- **Collection Organization**: Create and manage collections for better organization
- **Natural Language Queries**: Let AI assistants translate questions into SQL

### Authentication & Security
- **API Key Support**: Secure authentication via Metabase API keys (recommended)
- **Session-based Auth**: Alternative email/password authentication
- **Environment Variables**: Secure credential management via `.env` files

### AI Assistant Integration
- **Claude Desktop**: Native integration with Anthropic's Claude AI
- **Cursor IDE**: Seamless integration for AI-assisted development
- **Any MCP Client**: Compatible with all Model Context Protocol clients

### Enhanced Performance & Reliability
- **Context-aware Logging**: Real-time logging with debug, info, warning, and error levels visible to AI clients
- **Proper Error Handling**: FastMCP `ToolError` exceptions for better error messages and debugging
- **Middleware Stack**: Built-in error handling and logging middleware for production reliability
- **Best Practices**: Follows latest FastMCP patterns with duplicate prevention and clean configuration
- **Modern Python**: Uses Python 3.12+ type hints (`|` syntax) for better type safety

## Quick Start

### Prerequisites
- Python 3.12+
- Metabase instance with API access
- `uvx` or `uv` package manager

### Installation

#### Option 1: Using uvx (Easiest - No Installation Required)
```bash
# Run directly without installing (like npx for Python)
uvx metabase-mcp

# With environment variables
METABASE_URL=https://your-instance.com METABASE_API_KEY=your-key uvx metabase-mcp
```

#### Option 2: Install from PyPI
```bash
# Install globally
uv tool install metabase-mcp

# Or with pip
pip install metabase-mcp

# Then run
metabase-mcp
```

#### Option 3: Development Setup (From Source)
```bash
# Clone the repository
git clone https://github.com/cheukyin175/metabase-mcp.git
cd metabase-mcp

# Install dependencies
uv sync

# Run the server
uv run python server.py
```

## Configuration

Create a `.env` file with your Metabase credentials:

```bash
cp .env.example .env
```

### Configuration Options

#### Option 1: API Key Authentication (Recommended)
```env
METABASE_URL=https://your-metabase-instance.com
METABASE_API_KEY=your-api-key-here
```

#### Option 2: Email/Password Authentication
```env
METABASE_URL=https://your-metabase-instance.com
METABASE_USER_EMAIL=your-email@example.com
METABASE_PASSWORD=your-password
```

#### Optional: Custom Host/Port for SSE/HTTP
```env
HOST=localhost  # Default: 0.0.0.0
PORT=9000      # Default: 8000
```

## Usage

### Run the Server

#### Quick Start (No Setup Required)
```bash
# Run directly with uvx
uvx metabase-mcp

# With custom Metabase instance
METABASE_URL=https://your-instance.com METABASE_API_KEY=your-key uvx metabase-mcp
```

#### From Source (Development)
```bash
# STDIO transport (default)
uv run python server.py

# SSE transport (uses HOST=0.0.0.0, PORT=8000 by default)
uv run python server.py --sse

# HTTP transport (uses HOST=0.0.0.0, PORT=8000 by default)
uv run python server.py --http

# Custom host and port via environment variables
HOST=localhost PORT=9000 uv run python server.py --sse
HOST=192.168.1.100 PORT=8080 uv run python server.py --http
```

### Cursor Integration

You can manually configure Cursor by editing your Cursor settings.

**For SSE transport**: You must start the server before using Cursor:
```bash
uv run python server.py --sse
```

### Claude Desktop Integration

#### Option 1: Using uvx (Recommended)
Add this to `~/Library/Application Support/Claude/claude_desktop_config.json`:
```json
{
    "mcpServers": {
        "metabase-mcp": {
            "command": "uvx",
            "args": ["metabase-mcp"],
            "env": {
                "METABASE_URL": "https://your-metabase-instance.com",
                "METABASE_API_KEY": "your-api-key-here"
            }
        }
    }
}
```

#### Option 2: Using Local Installation
If you've cloned the repository:
```json
{
    "mcpServers": {
        "metabase-mcp": {
            "command": "uv",
            "args": [
                "run",
                "--directory",
                "/absolute/path/to/metabase-mcp",
                "python",
                "server.py"
            ],
            "env": {
                "METABASE_URL": "https://your-metabase-instance.com",
                "METABASE_API_KEY": "your-api-key-here"
            }
        }
    }
}
```

#### Option 3: Using FastMCP CLI
```bash
fastmcp install server.py -n "Metabase MCP"
```

## Available Tools

### Database Operations
| Tool | Description |
|------|------------|
| `list_databases` | List all configured databases in Metabase |
| `list_tables` | Get all tables in a specific database with metadata |
| `get_table_fields` | Retrieve field/column information for a table |
| `get_field_values` | Get distinct values for a field (useful for building filters) |

### Query Operations
| Tool | Description |
|------|------------|
| `execute_query` | Execute native SQL queries with parameter support |
| `execute_mbql_query` | Execute MBQL queries without saving (test queries) |
| `execute_card` | Run saved Metabase questions/cards |
| `get_mbql_reference` | Get MBQL syntax reference (for AI agents) |

### Card Management
| Tool | Description |
|------|------------|
| `list_cards` | List all saved questions/cards |
| `create_card` | Create new questions/cards with native SQL |
| `create_mbql_card` | Create new questions/cards with MBQL (Metabase Query Language) |

### Collection Management
| Tool | Description |
|------|------------|
| `list_collections` | Browse all collections |
| `create_collection` | Create new collections for organization |

### Dashboard Management
| Tool | Description |
|------|------------|
| `list_dashboards` | List all dashboards in Metabase |
| `get_dashboard` | Get full dashboard details including cards and parameters |
| `create_dashboard` | Create a new dashboard with optional filter parameters |
| `update_dashboard` | Update dashboard name, description, parameters, or archive it |
| `add_card_to_dashboard` | Add a card/question to a dashboard with position and size |
| `remove_card_from_dashboard` | Remove a card from a dashboard |
| `add_dashboard_filter` | Add or update filter parameters on a dashboard |
| `update_dashboard_cards` | Update card positions and parameter mappings |
| `copy_dashboard_tab` | Copy a tab from one dashboard to another with all cards and filters |

### Metric Management
| Tool | Description |
|------|------------|
| `list_metrics` | List all metrics in Metabase |
| `search_metrics_for_table` | Find metrics that use a specific table (call BEFORE creating cards) |
| `get_metric` | Get details of a specific metric |
| `create_metric` | Create a new metric with aggregation and optional filters |
| `update_metric` | Update a metric's name, description, formula, or filters |

### Card Creation (Metrics-First)
| Tool | Description |
|------|------------|
| `create_card_with_metrics` | Create a card using existing metrics (RECOMMENDED) |
| `create_mbql_card` | Create a card with direct MBQL (use only for exploratory queries) |

## Transport Methods

The server supports multiple transport methods:

- **STDIO** (default): For IDE integration (Cursor, Claude Desktop)
- **SSE**: Server-Sent Events for web applications
- **HTTP**: Standard HTTP for API access

```bash
uv run python server.py                        # STDIO (default)
uv run python server.py --sse                  # SSE (HOST=0.0.0.0, PORT=8000)
uv run python server.py --http                 # HTTP (HOST=0.0.0.0, PORT=8000)
HOST=localhost PORT=9000 uv run python server.py --sse   # Custom host/port
```

## Development

### Setup Development Environment

```bash
# Install with dev dependencies
uv sync --group dev

# Or with pip
pip install -r requirements-dev.txt
```

### Code Quality

```bash
# Run linting
uv run ruff check .

# Format code
uv run ruff format .

# Type checking
uv run mypy server.py
```

## Metrics-First Philosophy

This MCP server encourages a **metrics-first approach** to analytics:

1. **Check for existing metrics** before creating any aggregations
2. **Create reusable metrics** for important calculations (MQLs, Revenue, etc.)
3. **Use metrics in cards** instead of direct aggregations
4. **Ask for clarity** when column names are ambiguous

### Recommended Workflow

```python
# 1. First, check for existing metrics on the table
existing_metrics = await search_metrics_for_table(table_id=42)

# 2. If a suitable metric exists, use it
if any(m["name"] == "MQLs" for m in existing_metrics):
    mql_metric = next(m for m in existing_metrics if m["name"] == "MQLs")
    card = await create_card_with_metrics(
        name="MQLs by Month",
        database_id=1,
        source_table_id=42,
        metrics=[{"id": mql_metric["id"], "name": "MQLs"}],  # Name carries through
        breakouts=[["field", 100, {"temporal-unit": "month"}]],
        display="line"
    )

# 3. If no metric exists, create one first (ask user for confirmation)
else:
    # ASK USER: "No MQLs metric found. Should I create one?"
    metric = await create_metric(
        name="MQLs",
        database_id=1,
        source_table_id=42,
        aggregation=["count"],
        filters=["=", ["field", 105, None], True],
        description="Marketing Qualified Leads"
    )
    # Then use the new metric
    card = await create_card_with_metrics(
        name="MQLs by Month",
        database_id=1,
        source_table_id=42,
        metrics=[{"id": metric["id"], "name": "MQLs"}],
        breakouts=[["field", 100, {"temporal-unit": "month"}]],
        display="line"
    )
```

### Why Metrics-First?

- **Consistency**: Everyone uses the same calculation for "MQLs" or "Revenue"
- **Maintainability**: Update the metric once, all cards update automatically
- **Discoverability**: Metrics appear in "Common metrics" for easy reuse
- **Clear naming**: Metric names like "MQLs" carry through to visualizations

## Usage Examples

### SQL Query Examples

```python
# List all databases
databases = await list_databases()

# Execute a SQL query
result = await execute_query(
    database_id=1,
    query="SELECT * FROM users LIMIT 10"
)

# Create a card with SQL
card = await create_card(
    name="Active Users Report",
    database_id=1,
    query="SELECT COUNT(*) FROM users WHERE active = true",
    collection_id=2
)
```

### MBQL Query Examples

MBQL (Metabase Query Language) provides a database-agnostic way to build queries that integrate deeply with Metabase's features.

```python
# First, explore the schema to get field IDs
tables = await list_tables(database_id=1)
fields = await get_table_fields(table_id=5)
values = await get_field_values(field_id=12)  # Get distinct values for filters

# Test an MBQL query before saving
result = await execute_mbql_query(
    database_id=1,
    source_table_id=5,  # orders table
    aggregations=[["count"]],
    breakouts=[["field", 12, None]],  # group by status field
)

# Create a simple count by category
card = await create_mbql_card(
    name="Orders by Status",
    database_id=1,
    source_table_id=5,
    aggregations=[["count"]],
    breakouts=[["field", 12, None]],  # status field ID
    display="bar"
)

# Create a sum with filters and ordering
card = await create_mbql_card(
    name="Top 10 Products by Revenue",
    database_id=1,
    source_table_id=5,
    aggregations=[["sum", ["field", 8, None]]],  # sum of amount field
    breakouts=[["field", 15, None]],  # product_id field
    filters=[">=", ["field", 10, None], "2024-01-01"],  # date filter
    order_by=[["desc", ["aggregation", 0]]],  # order by sum descending
    limit=10,
    display="row",
    collection_id=2
)

# Group by time periods (month)
card = await create_mbql_card(
    name="Monthly Sales Trend",
    database_id=1,
    source_table_id=5,
    aggregations=[["sum", ["field", 8, None]]],
    breakouts=[["field", 10, {"temporal-unit": "month"}]],  # group by month
    order_by=[["asc", ["field", 10, {"temporal-unit": "month"}]]],
    display="line"
)

# Multiple aggregations
card = await create_mbql_card(
    name="Order Statistics by Category",
    database_id=1,
    source_table_id=5,
    aggregations=[
        ["count"],
        ["sum", ["field", 8, None]],
        ["avg", ["field", 8, None]]
    ],
    breakouts=[["field", 12, None]],
    display="table"
)

# Complex filters with AND/OR
card = await create_mbql_card(
    name="High Value Recent Orders",
    database_id=1,
    source_table_id=5,
    aggregations=[["count"]],
    filters=[
        "and",
        [">", ["field", 8, None], 1000],  # amount > 1000
        [">=", ["field", 10, None], "2024-01-01"]  # date >= 2024
    ],
    display="scalar"
)
```

### Dashboard Examples

```python
# List all dashboards
dashboards = await list_dashboards()

# Create a new dashboard
dashboard = await create_dashboard(
    name="Sales Overview",
    description="Key sales metrics and trends",
    collection_id=2
)
dashboard_id = dashboard["id"]

# Add cards to the dashboard
await add_card_to_dashboard(
    dashboard_id=dashboard_id,
    card_id=10,  # A previously created card
    size_x=6,
    size_y=4,
    row=0,
    col=0
)

await add_card_to_dashboard(
    dashboard_id=dashboard_id,
    card_id=11,
    size_x=6,
    size_y=4,
    row=0,
    col=6
)

# Add dashboard filters
await add_dashboard_filter(
    dashboard_id=dashboard_id,
    parameters=[
        {
            "id": "date_range",
            "name": "Date Range",
            "slug": "date",
            "type": "date/range",
            "sectionId": "date"
        },
        {
            "id": "category",
            "name": "Category",
            "slug": "category",
            "type": "string/=",
            "sectionId": "string"
        }
    ]
)

# Get full dashboard details (to find dashcard IDs)
dashboard = await get_dashboard(dashboard_id=dashboard_id)
dashcards = dashboard["dashcards"]

# Connect dashboard filters to card variables
await update_dashboard_cards(
    dashboard_id=dashboard_id,
    dashcards=[
        {
            "id": dashcards[0]["id"],
            "card_id": 10,
            "row": 0,
            "col": 0,
            "size_x": 6,
            "size_y": 4,
            "parameter_mappings": [
                {
                    "parameter_id": "date_range",
                    "card_id": 10,
                    "target": ["dimension", ["field", 25, None]]
                },
                {
                    "parameter_id": "category",
                    "card_id": 10,
                    "target": ["dimension", ["field", 30, None]]
                }
            ]
        }
    ]
)

# Remove a card from the dashboard
await remove_card_from_dashboard(
    dashboard_id=dashboard_id,
    dashcard_id=dashcards[1]["id"]
)

# Archive the dashboard
await update_dashboard(
    dashboard_id=dashboard_id,
    archived=True
)

# Copy a tab from one dashboard to another
# First, inspect the source dashboard to find tab IDs
source = await get_dashboard(dashboard_id=100)
# source["tabs"] might be: [{"id": 1, "name": "Overview"}, {"id": 2, "name": "Details"}]

# Copy the "Details" tab (id=2) to another dashboard
await copy_dashboard_tab(
    source_dashboard_id=100,
    target_dashboard_id=200,
    tab_id=2,
    new_tab_name="Copied Details",  # Optional rename
    include_filters=True  # Also copy the filters used by cards on this tab
)
```

### Metric Examples

Metrics are reusable aggregation definitions that standardize how important numbers are calculated.

```python
# Step 1: ALWAYS check for existing metrics first
existing = await search_metrics_for_table(table_id=42)
print(f"Found {len(existing)} metrics for this table")

# Step 2: If no suitable metric exists, create one
# (In practice, ASK THE USER before creating: "Should I create an MQLs metric?")
mql_metric = await create_metric(
    name="MQLs",
    database_id=1,
    source_table_id=42,  # gtm.leads table
    aggregation=["count"],
    filters=["=", ["field", 105, None], True],  # is_mql field
    description="Marketing Qualified Leads - count of leads where is_mql is true",
    collection_id=5
)

# Step 3: Use the metric in a card (name carries through!)
card = await create_card_with_metrics(
    name="MQLs by Month",
    database_id=1,
    source_table_id=42,
    metrics=[{"id": mql_metric["id"], "name": "MQLs"}],  # "MQLs" appears as column name
    breakouts=[["field", 100, {"temporal-unit": "month"}]],
    display="line",
    description="Monthly trend of Marketing Qualified Leads"
)

# More metric examples:

# Revenue metric with default time dimension
revenue_metric = await create_metric(
    name="Revenue",
    database_id=1,
    source_table_id=50,  # orders table
    aggregation=["sum", ["field", 200, None]],  # amount field
    description="Total revenue from all orders",
    default_time_dimension_field_id=201,  # created_at field
    default_time_dimension_unit="month"
)

# Average Order Value with filters
avg_order_metric = await create_metric(
    name="Average Order Value",
    database_id=1,
    source_table_id=50,
    aggregation=["avg", ["field", 200, None]],
    filters=[
        "and",
        ["=", ["field", 210, None], "completed"],
        [">", ["field", 200, None], 0]
    ],
    description="Average value of completed orders"
)

# Card with multiple metrics - names carry through
card = await create_card_with_metrics(
    name="Revenue Dashboard",
    database_id=1,
    source_table_id=50,
    metrics=[
        {"id": revenue_metric["id"], "name": "Total Revenue"},
        {"id": avg_order_metric["id"], "name": "Avg Order Value"}
    ],
    breakouts=[["field", 201, {"temporal-unit": "month"}]],
    display="combo"
)
```

### MBQL Reference

**Aggregations:**
- `["count"]` - Count rows
- `["sum", ["field", ID, null]]` - Sum of field
- `["avg", ["field", ID, null]]` - Average
- `["min", ["field", ID, null]]` - Minimum
- `["max", ["field", ID, null]]` - Maximum
- `["distinct", ["field", ID, null]]` - Distinct count

**Filters:**
- `["=", ["field", ID, null], "value"]` - Equals
- `["!=", ["field", ID, null], "value"]` - Not equals
- `[">", ["field", ID, null], 100]` - Greater than
- `["<", ["field", ID, null], 100]` - Less than
- `["between", ["field", ID, null], 1, 100]` - Between
- `["contains", ["field", ID, null], "text"]` - Contains
- `["is-null", ["field", ID, null]]` - Is null
- `["not-null", ["field", ID, null]]` - Is not null
- `["and", [...], [...]]` - AND conditions
- `["or", [...], [...]]` - OR conditions

**Temporal Grouping:**
- `["field", ID, {"temporal-unit": "day"}]`
- `["field", ID, {"temporal-unit": "week"}]`
- `["field", ID, {"temporal-unit": "month"}]`
- `["field", ID, {"temporal-unit": "quarter"}]`
- `["field", ID, {"temporal-unit": "year"}]`

## Project Structure

```
metabase-mcp/
├── server.py                 # Main MCP server implementation
├── pyproject.toml           # Project configuration and dependencies
└── .env.example             # Environment variables template
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details

## Resources

- [FastMCP Documentation](https://github.com/jlowin/fastmcp)
- [Model Context Protocol](https://modelcontextprotocol.io/)
- [Metabase API Documentation](https://www.metabase.com/docs/latest/api-documentation)
- [Claude Desktop Documentation](https://claude.ai/desktop)
- [Cursor IDE](https://cursor.sh/)

## Keywords & Topics

`metabase` `mcp` `model-context-protocol` `claude` `cursor` `ai-assistant` `fastmcp` `sql` `database` `analytics` `business-intelligence` `bi` `data-analysis` `anthropic` `llm` `python` `automation` `api` `data-science` `query-builder` `natural-language-sql`

## Star History

If you find this project useful, please consider giving it a star! It helps others discover this tool.

## Use Cases

- **Natural Language Database Queries**: Ask Claude to query your Metabase databases using plain English
- **Automated Report Generation**: Use AI to create and manage Metabase cards and collections
- **Data Exploration**: Let AI assistants help you discover insights from your data
- **SQL Query Assistance**: Get help writing and optimizing SQL queries through AI
- **Dashboard Management**: Automate the creation and organization of Metabase dashboards
- **Data Analysis Workflows**: Integrate AI-powered analytics into your development workflow 
