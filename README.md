# Dota 2 Pro Match Analysis Tool

A comprehensive tool for analyzing professional Dota 2 matches, focusing on team performance metrics, hero statistics, and meta trends.

## Features

- **Match Data Collection**: Fetch professional match data from OpenDota API
- **Team Performance Analysis**: Track win rates, draft patterns, and performance metrics
- **Hero Statistics**: Analyze hero pick/ban rates and success across different patches
- **Meta Trends Analysis**: Visualize changes in game duration, hero popularity, role distribution, and item usage over time
- **Interactive Filtering**: Filter analysis by date ranges, tournaments, teams, and patches

## Getting Started

### Prerequisites

- Python 3.7+
- Required Python packages (see requirements.txt)

### Installation

1. Clone the repository
   ```
   git clone https://github.com/jderrod/Dota2AnalysisTool.git
   ```

2. Create a virtual environment
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies
   ```
   pip install -r requirements.txt
   ```

4. Run the application
   ```
   python -m DotaGameAnalysis.frontend.main
   ```

## Architecture

The application follows a layered architecture:
- **Data Layer**: Database schema definitions and data access methods
- **Collection Layer**: OpenDota API integration and ETL processing
- **Analysis Layer**: Statistical calculations and pattern detection algorithms
- **Presentation Layer**: PyQt5-based UI with filtering controls and visualization components
