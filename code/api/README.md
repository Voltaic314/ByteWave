# ByteWave API

This is the API component of ByteWave, a data migration tool. The API provides endpoints to control and monitor data migrations.

## API Structure

The API follows a modular structure:

```
code/api/
├── main.go                 # Entry point for the API server
├── server/                 # Server configuration and setup
│   └── server.go           # Server implementation
└── routes/                 # API routes organized by feature
    └── migration/          # Migration-related routes
        ├── start/          # Start migration endpoint
        │   └── start.go
        ├── stop/           # Stop migration endpoint
        │   └── stop.go
        ├── pause/          # Pause migration endpoint
        │   └── pause.go
        ├── resume/         # Resume migration endpoint
        │   └── resume.go
        └── status/         # Migration status endpoint
            └── status.go
```

## API Endpoints

### Migration Control

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/migrations/start` | Start a new migration |
| POST | `/api/v1/migrations/stop` | Stop a running migration |
| POST | `/api/v1/migrations/pause` | Pause a running migration |
| POST | `/api/v1/migrations/resume` | Resume a paused migration |
| GET | `/api/v1/migrations/status` | Get migration status |

## Running the API

To run the API server:

```bash
go run code/api/main.go
```

The server will start on `http://localhost:8080`.

## Security

The API is designed to run on localhost only, making it accessible only from the same machine. Authentication will be added in a future update.

## Adding New Routes

To add a new route:

1. Create a new package under the appropriate feature directory
2. Implement the route handler and register function
3. Update the server.go file to include the new route

Example:

```go
// code/api/routes/migration/newfeature/newfeature.go
package newfeature

import (
    "net/http"
    "github.com/gin-gonic/gin"
)

// RegisterRoutes registers the new feature route
func RegisterRoutes(router *gin.RouterGroup) {
    router.POST("/newfeature", handleNewFeature)
}

// handleNewFeature handles the new feature request
func handleNewFeature(c *gin.Context) {
    // Implementation here
    c.JSON(http.StatusOK, gin.H{
        "status": "success",
        "message": "New feature executed successfully",
    })
}
```

Then update server.go to include the new route:

```go
// In setupRoutes() function
newfeature.RegisterRoutes(migrations)
```