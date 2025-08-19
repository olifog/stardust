# Demo

populate a small movies graph in Stardust using the python client

first, make sure the server is running

```bash
# run demo
STARDUST_URL="tcp://127.0.0.1:8080" uv run demo

# Linting
uvx ruff check src --fix

# Type checking
uvx mypy src
```
