"""
Player Time vs Stats Visualization

This module provides visualization components for displaying player time-series data
from Dota 2 matches, such as gold, XP, last hits, and denies over time.
"""

import os
import sys
import logging
from PyQt5.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
    QComboBox, QTabWidget, QGroupBox, QTableWidget, QTableWidgetItem,
    QHeaderView, QPushButton, QGridLayout, QSplitter
)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QColor

# Add matplotlib for visualizations
import matplotlib
matplotlib.use('Qt5Agg')  # Use Qt5 backend
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import numpy as np

# Configure logger
logger = logging.getLogger(__name__)

class TimeSeriesCanvas(FigureCanvas):
    """Canvas for displaying time series data"""
    def __init__(self, parent=None, width=8, height=4, dpi=100):
        self.fig = Figure(figsize=(width, height), dpi=dpi)
        self.axes = self.fig.add_subplot(111)
        super(TimeSeriesCanvas, self).__init__(self.fig)
        self.setParent(parent)

    def plot_data(self, times, values, label, color='blue', marker='o', line_style='-', clear=False):
        """Plot time series data on the canvas"""
        if clear:
            self.axes.clear()
        
        if times and values:
            self.axes.plot(times, values, marker=marker, linestyle=line_style, color=color, label=label)
            self.axes.set_xlabel('Game Time (minutes)')
            self.axes.set_ylabel(label)
            self.axes.grid(True, linestyle='--', alpha=0.7)
            
            # Format x-axis to show minutes
            def format_minutes(x, pos):
                minutes = int(x / 60)
                return f"{minutes}"
            
            import matplotlib.ticker as ticker
            self.axes.xaxis.set_major_formatter(ticker.FuncFormatter(format_minutes))
            
            # Add a legend if there's more than one dataset
            if self.axes.get_legend_handles_labels()[0]:
                self.axes.legend()
                
            self.fig.tight_layout()
            self.draw()

class EventTimelineCanvas(FigureCanvas):
    """Canvas for displaying event timeline (kills, purchases, runes)"""
    def __init__(self, parent=None, width=8, height=4, dpi=100):
        self.fig = Figure(figsize=(width, height), dpi=dpi)
        self.axes = self.fig.add_subplot(111)
        super(EventTimelineCanvas, self).__init__(self.fig)
        self.setParent(parent)
    
    def plot_events(self, event_times, event_types, event_details, clear=False):
        """Plot game events on a timeline"""
        if clear:
            self.axes.clear()
            
        if not event_times:
            return
            
        # Define colors for different event types
        event_colors = {
            'kill': 'red',
            'purchase': 'green',
            'rune': 'blue'
        }
        
        # Define markers for different event types
        event_markers = {
            'kill': 'X',
            'purchase': 's',
            'rune': '^'
        }
        
        # Convert time to minutes for display
        x_minutes = [t / 60 for t in event_times]
        
        # Create a scatter plot with events
        for i, (time, event_type, detail) in enumerate(zip(x_minutes, event_types, event_details)):
            color = event_colors.get(event_type, 'gray')
            marker = event_markers.get(event_type, 'o')
            
            self.axes.scatter(time, i % 5, color=color, marker=marker, s=100)
            
            # Add annotations for important events
            if event_type == 'kill':
                self.axes.annotate(f"Kill: {detail}", 
                                  (time, i % 5), 
                                  textcoords="offset points", 
                                  xytext=(5, 5),
                                  ha='left',
                                  fontsize=8)
            elif event_type == 'purchase' and "blink" in detail.lower() or "black_king_bar" in detail.lower():
                self.axes.annotate(f"Buy: {detail}", 
                                  (time, i % 5), 
                                  textcoords="offset points", 
                                  xytext=(5, -10),
                                  ha='left',
                                  fontsize=8)
                
        # Set up the axes
        self.axes.set_yticks([])  # Hide y-axis ticks
        self.axes.set_xlabel('Game Time (minutes)')
        self.axes.set_title('Game Events Timeline')
        self.axes.grid(True, axis='x', linestyle='--', alpha=0.7)
        
        # Create a legend for event types
        from matplotlib.lines import Line2D
        legend_elements = [
            Line2D([0], [0], marker=event_markers['kill'], color='w', markerfacecolor=event_colors['kill'], 
                  markersize=10, label='Kill'),
            Line2D([0], [0], marker=event_markers['purchase'], color='w', markerfacecolor=event_colors['purchase'], 
                  markersize=10, label='Item Purchase'),
            Line2D([0], [0], marker=event_markers['rune'], color='w', markerfacecolor=event_colors['rune'], 
                  markersize=10, label='Rune Pickup')
        ]
        self.axes.legend(handles=legend_elements, loc='upper right')
        
        self.fig.tight_layout()
        self.draw()

class PlayerTimevsStatsWindow(QMainWindow):
    """Window for displaying player time-series statistics"""
    
    def __init__(self, parent=None, match_id=None, player_id=None, player_name=None, player_slot=None):
        super().__init__(parent)
        
        self.match_id = match_id
        self.player_id = player_id
        self.player_name = player_name
        self.player_slot = player_slot
        
        # Set window properties
        self.setWindowTitle(f"Player Stats: {player_name} - Match {match_id}")
        self.setGeometry(150, 150, 1000, 800)
        
        # Create central widget and main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        # Header with player information
        header_layout = QHBoxLayout()
        
        # Player info
        info_group = QGroupBox("Player Information")
        info_layout = QGridLayout()
        info_layout.addWidget(QLabel("<b>Player:</b>"), 0, 0)
        info_layout.addWidget(QLabel(f"{player_name}"), 0, 1)
        info_layout.addWidget(QLabel("<b>Match ID:</b>"), 1, 0)
        info_layout.addWidget(QLabel(f"{match_id}"), 1, 1)
        info_layout.addWidget(QLabel("<b>Player Slot:</b>"), 2, 0)
        info_layout.addWidget(QLabel(f"{player_slot}"), 2, 1)
        
        # Determine if player is Radiant or Dire
        team = "Radiant" if int(player_slot) < 128 else "Dire"
        info_layout.addWidget(QLabel("<b>Team:</b>"), 3, 0)
        info_layout.addWidget(QLabel(team), 3, 1)
        
        info_group.setLayout(info_layout)
        header_layout.addWidget(info_group)
        
        # Visualization controls
        controls_group = QGroupBox("Visualization Controls")
        controls_layout = QGridLayout()
        
        # Stat selector
        controls_layout.addWidget(QLabel("Select Statistic:"), 0, 0)
        self.stat_combo = QComboBox()
        self.stat_combo.addItems(["Gold", "XP", "Last Hits", "Denies"])
        self.stat_combo.currentIndexChanged.connect(self.update_visualization)
        controls_layout.addWidget(self.stat_combo, 0, 1)
        
        # Event filter
        controls_layout.addWidget(QLabel("Event Filter:"), 1, 0)
        self.event_combo = QComboBox()
        self.event_combo.addItems(["All Events", "Kills Only", "Purchases Only", "Runes Only"])
        self.event_combo.currentIndexChanged.connect(self.update_event_timeline)
        controls_layout.addWidget(self.event_combo, 1, 1)
        
        controls_group.setLayout(controls_layout)
        header_layout.addWidget(controls_group)
        
        main_layout.addLayout(header_layout)
        
        # Create tabs for different visualizations
        self.tabs = QTabWidget()
        
        # Time series tab
        time_series_tab = QWidget()
        time_series_layout = QVBoxLayout()
        
        # Time series visualization
        self.time_series_canvas = TimeSeriesCanvas(time_series_tab)
        time_series_layout.addWidget(self.time_series_canvas)
        
        # Add event timeline
        self.event_canvas = EventTimelineCanvas(time_series_tab)
        time_series_layout.addWidget(self.event_canvas)
        
        time_series_tab.setLayout(time_series_layout)
        self.tabs.addTab(time_series_tab, "Time Series")
        
        # Raw data tab
        data_tab = QWidget()
        data_layout = QVBoxLayout()
        
        # Create table for raw time-series data
        self.data_table = QTableWidget()
        self.data_table.setColumnCount(6)
        self.data_table.setHorizontalHeaderLabels(["Time", "Gold", "XP", "Last Hits", "Denies", "Event"])
        self.data_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        data_layout.addWidget(self.data_table)
        
        data_tab.setLayout(data_layout)
        self.tabs.addTab(data_tab, "Raw Data")
        
        main_layout.addWidget(self.tabs)
        
        # Load the data
        self.load_player_timevs_stats()
    
    def load_player_timevs_stats(self):
        """Load time vs stats data for the player from the database"""
        try:
            # Import SQLAlchemy components
            from sqlalchemy import create_engine, text
            from sqlalchemy.orm import sessionmaker
            
            # Connect to the database
            db_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                'backend', 'database', 'data', 'dota_matches.db'
            )
            
            if not os.path.exists(db_path):
                logger.error(f"Database file does not exist at {db_path}")
                return
            
            # Create database connection
            engine = create_engine(f"sqlite:///{db_path}")
            Session = sessionmaker(bind=engine)
            session = Session()
            
            # Query time vs stats data
            query = text("""
                SELECT 
                    time, gold, xp, last_hits, denies, 
                    event_type, killed_hero, purchased_item, rune_type
                FROM 
                    pro_timevsstats
                WHERE 
                    match_id = :match_id AND player_id = :player_id
                ORDER BY 
                    time ASC
            """)
            
            results = session.execute(query, {"match_id": self.match_id, "player_id": self.player_id})
            
            # Create dictionaries to store data by time
            time_data = {}
            
            # Process all rows from the database
            for row in results:
                time = row[0]
                gold = row[1]
                xp = row[2]
                last_hits = row[3]
                denies = row[4]
                event_type = row[5]
                killed_hero = row[6]
                purchased_item = row[7]
                rune_type = row[8]
                
                # Initialize the time entry if it doesn't exist
                if time not in time_data:
                    time_data[time] = {
                        'gold': None,
                        'xp': None,
                        'last_hits': None,
                        'denies': None,
                        'events': []
                    }
                
                # Store values
                if gold is not None:
                    time_data[time]['gold'] = gold
                if xp is not None:
                    time_data[time]['xp'] = xp
                if last_hits is not None:
                    time_data[time]['last_hits'] = last_hits
                if denies is not None:
                    time_data[time]['denies'] = denies
                
                # Store event
                if event_type:
                    detail = None
                    if event_type == 'kill' and killed_hero:
                        detail = killed_hero
                    elif event_type == 'purchase' and purchased_item:
                        detail = purchased_item
                    elif event_type == 'rune' and rune_type:
                        detail = f"Rune {rune_type}"
                    
                    time_data[time]['events'].append({
                        'type': event_type,
                        'detail': detail
                    })
            
            # Initialize arrays for plotting
            self.times = []
            self.gold_values = []
            self.xp_values = []
            self.lh_values = []
            self.dn_values = []
            self.events = []
            
            # Clear existing rows in the table
            self.data_table.setRowCount(0)
            
            # Sort times to ensure chronological order
            sorted_times = sorted(time_data.keys())
            
            # Forward fill values (use previous value if missing)
            last_gold = 0
            last_xp = 0
            last_lh = 0
            last_dn = 0
            
            for time in sorted_times:
                data = time_data[time]
                
                # Update last known values if we have new data
                if data['gold'] is not None:
                    last_gold = data['gold']
                if data['xp'] is not None:
                    last_xp = data['xp']
                if data['last_hits'] is not None:
                    last_lh = data['last_hits']
                if data['denies'] is not None:
                    last_dn = data['denies']
                
                # Add values to our synchronized arrays
                self.times.append(time)
                self.gold_values.append(last_gold)
                self.xp_values.append(last_xp)
                self.lh_values.append(last_lh)
                self.dn_values.append(last_dn)
                
                # Store events in the flat structure
                for event in data['events']:
                    self.events.append({
                        'time': time,
                        'type': event['type'],
                        'detail': event['detail']
                    })
                
                # Add row to the data table
                row_index = self.data_table.rowCount()
                self.data_table.insertRow(row_index)
                
                # Format time as minutes:seconds
                minutes = time // 60
                seconds = time % 60
                time_str = f"{minutes:02d}:{seconds:02d}"
                
                # Add data to the table
                self.data_table.setItem(row_index, 0, QTableWidgetItem(time_str))
                self.data_table.setItem(row_index, 1, QTableWidgetItem(str(last_gold)))
                self.data_table.setItem(row_index, 2, QTableWidgetItem(str(last_xp)))
                self.data_table.setItem(row_index, 3, QTableWidgetItem(str(last_lh)))
                self.data_table.setItem(row_index, 4, QTableWidgetItem(str(last_dn)))
                
                # Add event information
                if data['events']:
                    event_texts = []
                    for event in data['events']:
                        event_text = event['type']
                        if event['detail']:
                            event_text += f": {event['detail']}"
                        event_texts.append(event_text)
                    
                    self.data_table.setItem(row_index, 5, QTableWidgetItem(", ".join(event_texts)))
            
            # Log array lengths for debugging
            logger.info(f"Loaded arrays with lengths: times={len(self.times)}, "  
                        f"gold={len(self.gold_values)}, xp={len(self.xp_values)}, "  
                        f"lh={len(self.lh_values)}, dn={len(self.dn_values)}")
            
            session.close()
            
            # Update the visualizations
            self.update_visualization()
            self.update_event_timeline()
            
        except Exception as e:
            logger.error(f"Error loading time vs stats data: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def update_visualization(self):
        """Update the time series visualization based on selected metric"""
        selected_stat = self.stat_combo.currentText()
        
        if not hasattr(self, 'times') or not self.times:
            logger.warning("No time series data available to plot")
            return
        
        # Select the appropriate data series
        if selected_stat == "Gold":
            values = self.gold_values
            color = 'gold'
            label = 'Gold'
        elif selected_stat == "XP":
            values = self.xp_values
            color = 'purple'
            label = 'Experience'
        elif selected_stat == "Last Hits":
            values = self.lh_values
            color = 'green'
            label = 'Last Hits'
        elif selected_stat == "Denies":
            values = self.dn_values
            color = 'red'
            label = 'Denies'
        else:
            logger.warning(f"Unknown stat type: {selected_stat}")
            return
        
        # Debug to check array lengths
        logger.info(f"Plotting {label} with arrays of length: times={len(self.times)}, values={len(values)}")
        
        # Make sure arrays have the same length
        if len(self.times) == len(values):
            try:
                # Plot the data
                self.time_series_canvas.plot_data(
                    self.times, values, label, color=color, clear=True
                )
            except Exception as e:
                logger.error(f"Error plotting data: {e}")
                import traceback
                logger.error(traceback.format_exc())
        else:
            # Try to fix the mismatch by trimming arrays to the same length
            logger.warning(f"Mismatched array lengths: times={len(self.times)}, values={len(values)}")
            try:
                # Use the shorter length
                min_length = min(len(self.times), len(values))
                logger.info(f"Trimming arrays to length {min_length}")
                self.time_series_canvas.plot_data(
                    self.times[:min_length], values[:min_length], label, color=color, clear=True
                )
            except Exception as e:
                logger.error(f"Error plotting data after trimming: {e}")
                import traceback
                logger.error(traceback.format_exc())
    
    def update_event_timeline(self):
        """Update the event timeline based on selected filter"""
        if not hasattr(self, 'events') or not self.events:
            return
        
        filter_option = self.event_combo.currentText()
        
        # Filter events based on selection
        filtered_events = []
        if filter_option == "All Events":
            filtered_events = self.events
        elif filter_option == "Kills Only":
            filtered_events = [e for e in self.events if e['type'] == 'kill']
        elif filter_option == "Purchases Only":
            filtered_events = [e for e in self.events if e['type'] == 'purchase']
        elif filter_option == "Runes Only":
            filtered_events = [e for e in self.events if e['type'] == 'rune']
        
        # Extract data for the plot
        event_times = [e['time'] for e in filtered_events]
        event_types = [e['type'] for e in filtered_events]
        event_details = [e['detail'] if e['detail'] else '' for e in filtered_events]
        
        # Plot the events
        self.event_canvas.plot_events(event_times, event_types, event_details, clear=True)
