# Dota 2 Match Analyzer Frontend

This is a desktop application that provides an interface for exploring and analyzing Dota 2 match data stored in your database.

## Features

- Browse professional Dota 2 matches with filtering options
- View user match history by Steam ID
- Analyze statistics and trends

## Requirements

- Python 3.7+
- PyQt5 (added to requirements.txt)
- All dependencies from the backend

## Getting Started

1. Install the required packages:
   ```
   pip install -r ../backend/requirements.txt
   ```

2. Run the application:
   ```
   python main.py
   ```

## Usage

### Pro Matches Tab
- Filter matches by date range, team, or league
- Double-click a match to view detailed information

### User Matches Tab
- Enter a Steam ID to view a player's match history
- Matches are loaded from the database and updated from the API as needed

### Statistics Tab
- Select different statistical views from the list
- View hero win rates, team performance, and meta trends
