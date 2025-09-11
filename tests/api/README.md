# MockFS API

A mock filesystem HTTP API for testing ByteWave traversal operations.

## Features

- **Batched List Operations**: Efficiently handles multiple concurrent folder listing requests
- **Path-Based Joins**: Uses database path joins for optimal performance
- **Mock File Downloads**: Generates random binary data for file downloads
- **Health Monitoring**: Built-in health check endpoint
- **Graceful Shutdown**: Proper cleanup on server termination

## API Endpoints

### Health Check
- `GET /health` - Returns server health status

### Filesystem Operations
- `GET /list/{path}` - List contents of a folder
- `GET /is-directory/{path}` - Check if path is a directory
- `POST /create-folder` - Create a new folder (not implemented)
- `POST /create-file` - Create a new file (not implemented)
- `GET /file/{fileID}/{filename}` - Get file download URL
- `GET /download/{fileID}/{filename}` - Download file content

## Usage

```bash
# Start the server
go run . -config ../traversal_tests/mock_fs_config.json

# Or with custom config
go run . -config /path/to/your/config.json
```

## Configuration

The server reads configuration from the same JSON file used by the seeder:

```json
{
  "databases": {
    "oracle": "tests/traversal_tests/oracle.db"
  },
  "network": {
    "address": "127.0.0.1",
    "port": 8086
  }
}
```

## Batching Strategy

The server uses a sophisticated batching system for list operations:

1. **Request Collection**: Multiple workers can request folder contents simultaneously
2. **Background Batching**: Requests are collected every 5ms or when batch size (10) is reached
3. **Single Query**: One database query with `UNION ALL` to get children from both tables
4. **Response Distribution**: Results are grouped by parent path and sent to respective workers

This approach provides:
- **High Concurrency**: Handles multiple workers efficiently
- **Database Efficiency**: Reduces query load with batching
- **Low Latency**: Fast response times for individual requests
