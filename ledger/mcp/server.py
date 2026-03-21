from fastmcp import FastMCP
from ledger.mcp.tools import register_tools
from ledger.mcp.resources import register_resources
import asyncio

# Create the FastMCP instance
mcp_server = FastMCP("TheLedger", dependencies=["fastmcp", "pydantic", "asyncpg"])

# In a real deployed MCP server, this would be initialized via lifespan/startup events 
# or dependency injection. For this architecture, we will provide a function to attach it.
mcp_store = None

def initialize_mcp(store):
    global mcp_store
    mcp_store = store
    register_tools(mcp_server, mcp_store)
    register_resources(mcp_server, mcp_store)

def main():
    # Typically one would initialize a DB connection pool and EventStore here
    # store = EventStore(pool)
    # initialize_mcp(store)
    mcp_server.run()

if __name__ == "__main__":
    main()
