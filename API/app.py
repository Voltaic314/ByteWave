'''
General API layout: 
API/
├── app.py                # Main Flask application entry point
├── oauth/
│   ├── __init__.py       # Imports all OAuth-related modules (optional)
│   ├── routes.py         # Routes for OAuth workflows
│   ├── utils.py          # Helper functions for token handling
├── migration/
│   ├── __init__.py       # Imports all migration-related modules (optional)
│   ├── routes.py         # Routes for migration actions
│   ├── tasks.py          # Background task management
├── reports/
│   ├── __init__.py       # Imports all report-related modules (optional)
│   ├── routes.py         # Routes for progress and completion reports
├── errors/
│   ├── __init__.py       # Imports all error-related modules (optional)
│   ├── routes.py         # Routes for retrying errors or reviewing logs
├── config.py             # Configuration settings (e.g., OAuth credentials, secrets)
'''


from flask import Flask
from API.oauth.routes import oauth_bp
from API.migration.routes import migration_bp
from API.reports.routes import reports_bp
from API.errors.routes import errors_bp

# Initialize Flask app
app = Flask(__name__)

# Register blueprints
app.register_blueprint(oauth_bp, url_prefix="/oauth")
app.register_blueprint(migration_bp, url_prefix="/migration")
app.register_blueprint(reports_bp, url_prefix="/reports")
app.register_blueprint(errors_bp, url_prefix="/errors")

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000)
