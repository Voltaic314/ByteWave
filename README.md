# ByteWave
This application allows you to migrate data from one storage service to another seamlessly. Allowing for extra customization along the way if you so choose. :) 

![Main Logo](/assets/logo_large_with_text_overlay.png)

## WORK IN PROGRESS ## 
This application is currently being developed and is currently not in a working state. Please do not try to use or install it until this is updated or told otherwise by the developers. Exciting things to come soon!

## API Documentation

ByteWave provides a RESTful API for controlling data migrations. The API is designed to be simple, secure, and easy to use.

### Security

The API is configured to only accept connections from localhost (127.0.0.1), ensuring that only applications running on the same machine can access it. This is a security measure to prevent unauthorized access.

### API Endpoints

All endpoints are prefixed with `/api/v1`.

#### Migration Control

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/migrations/start` | POST | Start a new migration |
| `/migrations/stop` | POST | Stop a running migration |
| `/migrations/pause` | POST | Pause a running migration |
| `/migrations/resume` | POST | Resume a paused migration |
| `/migrations/status` | GET | Get the current migration status |

### Request/Response Format

All requests and responses use JSON format.

#### Example Requests

Start a migration:
```bash
curl -X POST http://localhost:8080/api/v1/migrations/start
```

Stop a migration:
```bash
curl -X POST http://localhost:8080/api/v1/migrations/stop
```

Get migration status:
```bash
curl http://localhost:8080/api/v1/migrations/status
```

#### Example Response

```json
{
  "status": "success",
  "data": {
    "migration_status": "running",
    "progress": 45,
    "files_processed": 123,
    "total_files": 275,
    "current_file": "documents/reports/annual_2024.pdf",
    "errors": []
  }
}
```

### Running the API Server

To start the API server:

```bash
go run code/api/main.go
```

The server will start on port 8080 by default.
