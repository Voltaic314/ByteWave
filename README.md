# ByteWave
This application allows you to migrate data from one storage service to another seamlessly. Allowing for extra customization along the way if you so choose. :) 

![Main Logo](/assets/logo_large_with_text_overlay.png)

## WORK IN PROGRESS ## 
This application is currently being developed and is currently not in a working state. Please do not try to use or install it until this is updated or told otherwise by the developers. Exciting things to come soon!

## API Documentation

ByteWave provides a RESTful API for controlling data migrations. The API is designed to be used by the ByteWave UI and is only accessible from localhost for security reasons.

### Base URL

```
http://localhost:8080/api/v1
```

### Endpoints

#### Start Migration

Start a new data migration.

- **URL**: `/migrations/start`
- **Method**: `POST`
- **Request Body**: Empty
- **Response**:
  ```json
  {
    "status": "success",
    "message": "Migration started successfully"
  }
  ```

#### Stop Migration

Stop a running migration.

- **URL**: `/migrations/stop`
- **Method**: `POST`
- **Request Body**: Empty
- **Response**:
  ```json
  {
    "status": "success",
    "message": "Migration stopped successfully"
  }
  ```

#### Pause Migration

Pause a running migration.

- **URL**: `/migrations/pause`
- **Method**: `POST`
- **Request Body**: Empty
- **Response**:
  ```json
  {
    "status": "success",
    "message": "Migration paused successfully"
  }
  ```

#### Resume Migration

Resume a paused migration.

- **URL**: `/migrations/resume`
- **Method**: `POST`
- **Request Body**: Empty
- **Response**:
  ```json
  {
    "status": "success",
    "message": "Migration resumed successfully"
  }
  ```

#### Get Migration Status

Get the current status of a migration.

- **URL**: `/migrations/status`
- **Method**: `GET`
- **Response**:
  ```json
  {
    "status": "success",
    "data": {
      "migration_status": "running",
      "progress": 45,
      "files_processed": 450,
      "total_files": 1000,
      "errors": []
    }
  }
  ```

### Running the API Server

To start the API server:

```bash
go run code/api/main.go
```
