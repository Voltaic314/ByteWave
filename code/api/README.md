# ByteWave API

This is the API component of ByteWave, providing endpoints to control and monitor data migrations.

## Overview

The ByteWave API is built using the Gin framework and follows a modular design pattern. It's designed to be:

- **Secure**: Only accessible from localhost by default
- **Modular**: Easy to add new routes and functionality
- **RESTful**: Following standard REST conventions
- **Extensible**: Structured to accommodate future growth

## API Endpoints

### Migration Control

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/migrations/start` | Start a new migration |
| POST | `/api/v1/migrations/stop` | Stop a running migration |
| POST | `/api/v1/migrations/pause` | Pause a running migration |
| POST | `/api/v1/migrations/resume` | Resume a paused migration |
| GET | `/api/v1/migrations/status` | Get the current migration status |

### Request/Response Examples

#### Start Migration

**Request:**
```http
POST /api/v1/migrations/start
Content-Type: application/json

{}
```

**Response:**
```json
{
  "status": "success",
  "message": "Migration started successfully"
}
```

#### Stop Migration

**Request:**
```http
POST /api/v1/migrations/stop
Content-Type: application/json

{}
```

**Response:**
```json
{
  "status": "success",
  "message": "Migration stopped successfully"
}
```

#### Get Migration Status

**Request:**
```http
GET /api/v1/migrations/status
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "migrationStatus": "running"
  }
}
```

## Running the API

To start the API server:

```bash
go run code/api/main.go
```

The server will start on `127.0.0.1:8080` by default.

## Project Structure

```
code/api/
├── main.go                 # Entry point for the API server
├── server/
│   └── server.go           # Server configuration and setup
├── routes/
│   └── migration.go        # Route definitions for migrations
└── handlers/
    └── migration/
        └── migration.go    # Handler implementations for migration endpoints
```

## Adding New Routes

To add new functionality to the API:

1. Create a new handler package in `handlers/`
2. Create a new route file in `routes/`
3. Register the new routes in `server/server.go`

## Future Enhancements

- Authentication and authorization
- Rate limiting
- Detailed logging
- Configuration endpoints
- Webhook notifications