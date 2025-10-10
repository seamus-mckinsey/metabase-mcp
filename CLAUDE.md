# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Model Context Protocol (MCP) server for Metabase, built with FastMCP and Python. It enables AI assistants to interact with Metabase instances for database analytics, query execution, and visualization management.

## Development Commands

### Setup and Installation
```bash
# Install dependencies (recommended)
uv sync

# Install with dev dependencies
uv sync --group dev

# Alternative with pip
pip install -r requirements.txt
```

### Running the Server
```bash
# STDIO transport (default - for IDE integration)
uv run python server.py

# SSE transport (Server-Sent Events)
uv run python server.py --sse

# HTTP transport
uv run python server.py --http

# Custom host/port via environment variables
HOST=localhost PORT=9000 uv run python server.py --sse
```

### Code Quality
```bash
# Linting
uv run ruff check .

# Format code
uv run ruff format .

# Type checking
uv run mypy server.py

# Run tests
uv run pytest -v

# Run with coverage
uv run pytest --cov=server --cov-report=html
```

## Architecture

### Core Components

**server.py** - Single-file MCP server implementation containing:
- `MetabaseClient`: HTTP client with dual authentication support (API key or session-based)
- Tool definitions organized by domain (Database, Query, Card, Collection operations)
- Transport layer supporting STDIO, SSE, and HTTP

### Authentication Flow

The server supports two authentication methods with automatic fallback:
1. **API Key** (preferred): Direct authentication via `METABASE_API_KEY` environment variable
2. **Session-based**: Email/password authentication with session token management

Authentication is determined at initialization in `MetabaseClient.__init__()` and handled via `_get_headers()` method.

### Tool Categories

**Database Operations** (server.py:131-233):
- `list_databases`: Returns all configured databases
- `list_tables`: Returns formatted markdown table of database tables
- `get_table_fields`: Returns field metadata with pagination support (default limit: 20)

**Query Operations** (server.py:240-271):
- `execute_query`: Executes native SQL with optional parameter support

**Card/Question Operations** (server.py:278-366):
- `list_cards`: Lists all saved questions
- `execute_card`: Executes saved questions with parameters
- `create_card`: Creates new questions with visualization settings

**Collection Operations** (server.py:373-422):
- `list_collections`: Lists all collections
- `create_collection`: Creates new collections with parent/child hierarchy support

### Transport Methods

The server supports three transport protocols:
- **STDIO**: Default mode for Claude Desktop and Cursor integration
- **SSE**: For web applications needing real-time updates
- **HTTP**: Standard REST-like API access

Transport selection is handled in `main()` via command-line arguments.

## Configuration

Environment variables are loaded from `.env` file (use `.env.example` as template):

**Required**:
- `METABASE_URL`: Base URL of Metabase instance

**Authentication** (one required):
- `METABASE_API_KEY`: API key (preferred)
- OR `METABASE_USER_EMAIL` + `METABASE_PASSWORD`: Session auth

**Optional**:
- `HOST`: Server host for SSE/HTTP (default: 0.0.0.0)
- `PORT`: Server port for SSE/HTTP (default: 8000)

## Key Implementation Details

### Error Handling
All tool functions include try/except blocks that log errors via Python's logging module and re-raise exceptions for MCP error handling.

### Async Architecture
The entire codebase is async-first using httpx.AsyncClient for HTTP operations and FastMCP's async tool decorators.

### Resource Cleanup
The `cleanup()` function ensures proper HTTP client closure on shutdown, registered via asyncio.run() in the finally block.

## Testing

Tests should be added to a `tests/` directory (currently not present in codebase). Use pytest with async support:
```bash
uv run pytest -v
```
