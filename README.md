# Metabase MCP Server - Connect AI Assistants to Your Metabase Analytics 🚀

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![FastMCP](https://img.shields.io/badge/FastMCP-v0.19+-green.svg)](https://github.com/jlowin/fastmcp)

A high-performance **Model Context Protocol (MCP) server** for **Metabase**, enabling AI assistants like **Claude**, **Cursor**, and other MCP clients to interact seamlessly with your Metabase instance. Query databases, execute SQL, manage dashboards, and automate analytics workflows with natural language through AI-powered database operations.

**Perfect for:** Data analysts, developers, and teams looking to integrate AI assistants with their Metabase business intelligence platform for automated SQL queries, dashboard management, and data exploration.

## ✨ Key Features

### 🗄️ Database Operations
- **List Databases**: Browse all configured Metabase databases
- **Table Discovery**: Explore tables with metadata and descriptions
- **Field Inspection**: Get detailed field/column information with smart pagination

### 📊 Query & Analytics
- **SQL Execution**: Run native SQL queries with parameter support and templating
- **Card Management**: Execute, create, and manage Metabase questions/cards
- **Collection Organization**: Create and manage collections for better organization
- **Natural Language Queries**: Let AI assistants translate questions into SQL

### 🔐 Authentication & Security
- **API Key Support**: Secure authentication via Metabase API keys (recommended)
- **Session-based Auth**: Alternative email/password authentication
- **Environment Variables**: Secure credential management via `.env` files

### 🤖 AI Assistant Integration
- **Claude Desktop**: Native integration with Anthropic's Claude AI
- **Cursor IDE**: Seamless integration for AI-assisted development
- **Any MCP Client**: Compatible with all Model Context Protocol clients

## 🚀 Quick Start

### Prerequisites
- Python 3.12+
- Metabase instance with API access
- uv package manager (recommended) or pip

### Installation

#### Using uv (Recommended)
```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone the repository
git clone https://github.com/yourusername/metabase-mcp.git
cd metabase-mcp

# Install dependencies
uv sync
```

#### Using pip
```bash
# Clone and install
git clone https://github.com/yourusername/metabase-mcp.git
cd metabase-mcp
pip install -r requirements.txt
```

## ⚙️ Configuration

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

# Set environment variables persistently
export HOST=localhost
export PORT=9000
uv run python server.py --sse
```

### FastMCP CLI Integration

```bash
# Run with FastMCP CLI
fastmcp run server.py

# Install as Claude Desktop MCP server
fastmcp install server.py -n "Metabase MCP"
```

### Cursor Integration

You can manually configure Cursor by editing your Cursor settings. Example configurations are available in the `config/` directory.

**For SSE transport**: You must start the server before using Cursor:
```bash
uv run python server.py --sse 8000
```

### Claude Integration
After running `uv sync`, you can find the Python executable at `/path/to/repo/.venv/bin/python`.
To integrate with Claude, add or update the configuration file at `~/Library/Application\ Support/Claude/claude_desktop_config.json`:
```json
{
    "mcpServers": {
        "metabase-mcp-server": {
            "command": "/path/to/repo/.venv/bin/python",
            "args": ["/path/to/repo/server.py"]
        }
    }
}
```

## 🛠️ Available Tools

### Database Operations
| Tool | Description |
|------|------------|
| `list_databases` | List all configured databases in Metabase |
| `list_tables` | Get all tables in a specific database with metadata |
| `get_table_fields` | Retrieve field/column information for a table |

### Query Operations
| Tool | Description |
|------|------------|
| `execute_query` | Execute native SQL queries with parameter support |
| `execute_card` | Run saved Metabase questions/cards |

### Card Management
| Tool | Description |
|------|------------|
| `list_cards` | List all saved questions/cards |
| `create_card` | Create new questions/cards with SQL queries |

### Collection Management
| Tool | Description |
|------|------------|
| `list_collections` | Browse all collections |
| `create_collection` | Create new collections for organization |

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

## 🧪 Development

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


## 📚 Usage Examples

### Query Examples

```python
# List all databases
databases = await list_databases()

# Execute a SQL query
result = await execute_query(
    database_id=1,
    query="SELECT * FROM users LIMIT 10"
)

# Create and run a card
card = await create_card(
    name="Active Users Report",
    database_id=1,
    query="SELECT COUNT(*) FROM users WHERE active = true",
    collection_id=2
)
```

## 📁 Project Structure

```
metabase-mcp/
├── server.py                 # Main MCP server implementation
├── setup.py                 # Setup and installation script
├── pyproject.toml           # Project configuration and dependencies
├── .env.example             # Environment variables template
└── config/                  # Configuration examples for IDE integration
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

MIT License - see LICENSE file for details

## 🔗 Resources

- [FastMCP Documentation](https://github.com/jlowin/fastmcp)
- [Model Context Protocol](https://modelcontextprotocol.io/)
- [Metabase API Documentation](https://www.metabase.com/docs/latest/api-documentation)
- [Claude Desktop Documentation](https://claude.ai/desktop)
- [Cursor IDE](https://cursor.sh/)

## 🏷️ Keywords & Topics

`metabase` `mcp` `model-context-protocol` `claude` `cursor` `ai-assistant` `fastmcp` `sql` `database` `analytics` `business-intelligence` `bi` `data-analysis` `anthropic` `llm` `python` `automation` `api` `data-science` `query-builder` `natural-language-sql`

## ⭐ Star History

If you find this project useful, please consider giving it a star! It helps others discover this tool.

## 🔍 Use Cases

- **Natural Language Database Queries**: Ask Claude to query your Metabase databases using plain English
- **Automated Report Generation**: Use AI to create and manage Metabase cards and collections
- **Data Exploration**: Let AI assistants help you discover insights from your data
- **SQL Query Assistance**: Get help writing and optimizing SQL queries through AI
- **Dashboard Management**: Automate the creation and organization of Metabase dashboards
- **Data Analysis Workflows**: Integrate AI-powered analytics into your development workflow 
