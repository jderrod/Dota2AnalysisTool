from flask import Flask
from flask_cors import CORS
import logging
import os
from api.player_routes import player_routes

# Configure logging to suppress database connection messages
logging.getLogger('sqlite3').setLevel(logging.ERROR)

# Create Flask application
app = Flask(__name__)

# Enable CORS for frontend requests
CORS(app, resources={r"/*": {"origins": "*"}})

# Register blueprints
app.register_blueprint(player_routes)

# Create data/databases directory structure if it doesn't exist
data_dir = os.path.join(os.path.dirname(__file__), 'data')
databases_dir = os.path.join(data_dir, 'databases')

if not os.path.exists(data_dir):
    os.makedirs(data_dir)
    
if not os.path.exists(databases_dir):
    os.makedirs(databases_dir)

@app.route('/')
def index():
    return {"status": "API is running", "version": "1.0.0"}

if __name__ == '__main__':
    app.run(debug=True, port=5000)
