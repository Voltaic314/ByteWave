# ByteWave API

This directory contains the API server for ByteWave, which provides endpoints for controlling data migrations.

## Overview

The API is built using the Gin web framework and provides the following functionality:

- Start a new migration
- Stop a running migration
- Pause a running migration
- Resume a paused migration
- Get the status of a migration

The API is designed to be modular and extensible, making it easy to add new endpoints in the future.

## API Endpoints

All endpoints are prefixed with `/api/v1`.

### Migration Control

#### Start a Migration

```
POST /api/v1/migrations/start
```

Request body:
```json
{
  "sourcePath": "/path/to/source",
  "destinationPath": "/path/to/destination",
  "includePatterns": ["*.jpg", "*.png"],
  "excludePatterns": ["*.tmp"],
  "chunkSize": 1048576
}
```

Response:
```json
{
  "message": "Migration started successfully",
  "id": "550e8400-e29b-41d4-a716-446655440000"
}
```

#### Stop a Migration

```
POST /api/v1/migrations/stop
```

Request body (optional):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000"
}
```

If no ID is provided in the request body, the current active migration will be stopped.

Response:
```json
{
  "message": "Migration stopped successfully"
}
```

#### Pause a Migration

```
POST /api/v1/migrations/pause
```

Request body (optional):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000"
}
```

If no ID is provided in the request body, the current active migration will be paused.

Response:
```json
{
  "message": "Migration paused successfully"
}
```

#### Resume a Migration

```
POST /api/v1/migrations/resume
```

Request body (optional):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000"
}
```

If no ID is provided in the request body, the current active migration will be resumed.

Response:
```json
{
  "message": "Migration resumed successfully"
}
```

#### Get Migration Status

```
GET /api/v1/migrations/status
```

Query parameter (optional):
```
?id=550e8400-e29b-41d4-a716-446655440000
```

If no ID is provided as a query parameter, the status of the current active migration will be returned.

Response:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running",
  "config": {
    "sourcePath": "/path/to/source",
    "destinationPath": "/path/to/destination",
    "includePatterns": ["*.jpg", "*.png"],
    "excludePatterns": ["*.tmp"],
    "chunkSize": 1048576
  },
  "startTime": "2025-01-01T00:00:00Z",
  "totalFiles": 100,
  "processedFiles": 50,
  "errorCount": 0
}
```

## Security

The API server only binds to localhost (127.0.0.1), ensuring that it can only be accessed from the same machine. This is a security measure to prevent unauthorized access to the API.

In the future, authentication and authorization mechanisms will be added to secure the API further.

## Running the API Server

To run the API server:

```bash
go run code/cmd/api/main.go
```

By default, the server runs on port 8080. You can specify a different port using the `-port` flag:

```bash
go run code/cmd/api/main.go -port 3000
```

## Future Enhancements

- Authentication and authorization
- Rate limiting
- Detailed logging
- Metrics and monitoring
- WebSocket support for real-time updates
- Additional endpoints for configuration management