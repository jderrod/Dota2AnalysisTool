import schedule
import time
import logging
from database.database_pro_teams import DotaDatabase
import sys

# Configure logging to file instead of console to reduce clutter
logging.basicConfig(
    filename='database_updates.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def update_job():
    try:
        logging.info("Starting scheduled database update")
        db = DotaDatabase()
        # Redirect stdout to suppress connection messages
        original_stdout = sys.stdout
        sys.stdout = open('temp_output.txt', 'w')
        
        db.update_all_tables()
        
        # Restore stdout
        sys.stdout.close()
        sys.stdout = original_stdout
        
        logging.info("Database update completed successfully")
    except Exception as e:
        logging.error(f"Error during database update: {str(e)}")

# Schedule the job to run daily at 11:59 PM
schedule.every().day.at("23:59").do(update_job)

if __name__ == "__main__":
    logging.info("Scheduler started")
    print("Database update scheduler is running...")
    print("Updates will occur daily at 11:59 PM")
    print("Press Ctrl+C to exit")
    
    # Run the update immediately on startup (optional)
    # update_job()
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute
