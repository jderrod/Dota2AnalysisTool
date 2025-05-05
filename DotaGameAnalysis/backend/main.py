"""
Dota 2 Professional Match Data Scraper and Analyzer

This is the main entry point for the Dota 2 professional match data system.
It combines scraping, database storage, and analysis functionality.
"""
import os
import argparse
import logging
from datetime import datetime, timedelta
from scraper import DotaMatchScraper
from database import DotaDatabase
from analysis import DotaAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("dota_analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def scrape_data(start_date, end_date, min_league_tier=1, individual_files=True, api_key=None):
    """
    Scrape professional match data from the OpenDota API.
    
    Args:
        start_date (str): Start date in ISO format (YYYY-MM-DD).
        end_date (str): End date in ISO format (YYYY-MM-DD).
        min_league_tier (int, optional): Minimum league tier to include. Defaults to 1.
        individual_files (bool, optional): Save each match in its own file. Defaults to True.
        api_key (str, optional): OpenDota API key for premium access. Defaults to None.
    """
    logger.info(f"Scraping match data from {start_date} to {end_date}")
    
    # Initialize scraper
    scraper = DotaMatchScraper(api_key=api_key)
    
    # Get match summaries
    matches = scraper.get_pro_matches_by_date_range(start_date, end_date, min_league_tier)
    
    if not matches:
        logger.warning("No matches found for the specified date range.")
        return
    
    # Create data directories
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'raw')
    matches_dir = os.path.join(data_dir, 'matches')
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(matches_dir, exist_ok=True)
    
    # Save match summaries to JSON
    current_date = datetime.now().strftime("%Y%m%d")
    summary_file = os.path.join(data_dir, f"pro_matches_{start_date}_to_{end_date}.json")
    scraper.save_to_json(matches, summary_file)
    
    # Store in database
    db = DotaDatabase()
    db.create_tables()
    db.batch_insert_matches(matches)
    
    # Get detailed data for a subset of matches
    logger.info("Fetching detailed match data")
    matches_to_analyze = min(50, len(matches))  # Limit to 50 matches for demo purposes
    match_ids = [match['match_id'] for match in matches[:matches_to_analyze]]
    
    match_details = scraper.get_match_details_batch(
        match_ids,
        save_individual_files=individual_files,
        directory=matches_dir
    )
    
    if match_details:
        # Save match details to a single JSON (in addition to individual files)
        if not individual_files:
            details_file = os.path.join(data_dir, f"match_details_{start_date}_to_{end_date}.json")
            scraper.save_to_json(match_details, details_file)
        
        # Store in database
        db.batch_insert_match_details(match_details)
    
    logger.info(f"Data scraping complete. Scraped {len(matches)} match summaries and {len(match_details)} match details.")


def scrape_recent_pro_matches(limit=5, use_checkpoint=True, min_tier=None, individual_files=True, api_key=None):
    """
    Scrape the most recent professional matches from the OpenDota API.
    
    Args:
        limit (int, optional): Number of matches to retrieve. Defaults to 5.
        use_checkpoint (bool, optional): Whether to use checkpoint data. Defaults to True.
        min_tier (int, optional): If specified, filter for matches of this tier or higher.
        individual_files (bool, optional): Save each match in its own file. Defaults to True.
        api_key (str, optional): OpenDota API key for premium access. Defaults to None.
    """
    logger.info(f"Scraping {limit} recent professional matches")
    
    # Initialize scraper
    scraper = DotaMatchScraper(api_key=api_key)
    
    # Load checkpoint if needed
    last_match_id = None
    if use_checkpoint:
        last_match_id = scraper.load_checkpoint()
    
    # Get recent pro matches
    matches, last_processed_id = scraper.get_recent_pro_matches(limit=limit, last_match_id=last_match_id)
    
    if not matches:
        logger.warning("No professional matches found.")
        return
        
    # Filter by tier if needed
    if min_tier is not None:
        matches = [m for m in matches if m.get('league_tier', 0) >= min_tier]
        logger.info(f"Filtered to {len(matches)} matches with tier >= {min_tier}")
        
        if not matches:
            logger.warning(f"No matches found with tier >= {min_tier}.")
            return
    
    # Save the new checkpoint
    if last_processed_id and use_checkpoint:
        scraper.save_checkpoint(last_processed_id)
    
    # Create data directories
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'raw')
    matches_dir = os.path.join(data_dir, 'matches')
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(matches_dir, exist_ok=True)
    
    # Save match summaries to JSON
    current_date = datetime.now().strftime("%Y%m%d")
    summary_file = os.path.join(data_dir, f"recent_pro_matches_{current_date}.json")
    scraper.save_to_json(matches, summary_file)
    
    # Store in database
    db = DotaDatabase()
    db.create_tables()
    db.batch_insert_matches(matches)
    
    # Get detailed data for all matches
    logger.info("Fetching detailed match data")
    match_ids = [match['match_id'] for match in matches]
    
    match_details = scraper.get_match_details_batch(
        match_ids,
        save_individual_files=individual_files,
        directory=matches_dir
    )
    
    if match_details:
        # Save match details to a single JSON (in addition to individual files)
        if not individual_files:
            details_file = os.path.join(data_dir, f"recent_match_details_{current_date}.json")
            scraper.save_to_json(match_details, details_file)
        
        # Store in database
        db.batch_insert_match_details(match_details)
    
    logger.info(f"Data scraping complete. Scraped {len(matches)} match summaries and {len(match_details)} match details.")
    return matches, match_details


def analyze_data():
    """
    Analyze the Dota 2 match data in the database and generate reports.
    """
    logger.info("Starting data analysis")
    
    # Initialize analyzer
    analyzer = DotaAnalyzer()
    
    # Generate comprehensive report
    report_path = analyzer.generate_comprehensive_report()
    
    logger.info(f"Analysis complete. Report generated at {report_path}")
    
    # Return the path for potential opening in a browser
    return report_path


def main():
    """Main function to parse arguments and execute appropriate functionality."""
    parser = argparse.ArgumentParser(description="Dota 2 Professional Match Data Scraper and Analyzer")
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Scrape command
    scrape_parser = subparsers.add_parser("scrape", help="Scrape match data")
    scrape_parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    scrape_parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    scrape_parser.add_argument("--tier", type=int, default=None, help="Minimum league tier (1-3)")
    scrape_parser.add_argument("--recent", type=int, help="Number of recent matches to retrieve")
    scrape_parser.add_argument("--no-checkpoint", action="store_true", help="Don't use checkpoint data for recent matches")
    scrape_parser.add_argument("--no-individual-files", action="store_true", help="Don't save individual match files")
    scrape_parser.add_argument("--api-key", type=str, help="OpenDota API key for premium access")
    
    # Analyze command
    analyze_parser = subparsers.add_parser("analyze", help="Analyze stored match data")
    
    # Full pipeline command
    pipeline_parser = subparsers.add_parser("pipeline", help="Run the full pipeline")
    pipeline_parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    pipeline_parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    pipeline_parser.add_argument("--tier", type=int, default=None, help="Minimum league tier (1-3)")
    pipeline_parser.add_argument("--recent", type=int, help="Number of recent matches to retrieve")
    pipeline_parser.add_argument("--no-checkpoint", action="store_true", help="Don't use checkpoint data for recent matches")
    pipeline_parser.add_argument("--no-individual-files", action="store_true", help="Don't save individual match files")
    pipeline_parser.add_argument("--api-key", type=str, help="OpenDota API key for premium access")
    
    args = parser.parse_args()
    
    if args.command == "scrape":
        if args.recent:
            scrape_recent_pro_matches(
                limit=args.recent, 
                use_checkpoint=not args.no_checkpoint, 
                min_tier=args.tier,
                individual_files=not args.no_individual_files,
                api_key=args.api_key
            )
        elif args.start and args.end:
            scrape_data(
                args.start, 
                args.end, 
                args.tier or 1,
                individual_files=not args.no_individual_files,
                api_key=args.api_key
            )
        else:
            # Default to scraping 5 recent matches
            scrape_recent_pro_matches(
                individual_files=not args.no_individual_files,
                api_key=args.api_key
            )
    elif args.command == "analyze":
        analyze_data()
    elif args.command == "pipeline":
        if args.recent:
            matches, details = scrape_recent_pro_matches(
                limit=args.recent, 
                use_checkpoint=not args.no_checkpoint, 
                min_tier=args.tier,
                individual_files=not args.no_individual_files,
                api_key=args.api_key
            )
            if matches and details:
                analyze_data()
        elif args.start and args.end:
            scrape_data(
                args.start, 
                args.end, 
                args.tier or 1,
                individual_files=not args.no_individual_files,
                api_key=args.api_key
            )
            analyze_data()
        else:
            # Default to scraping 5 recent matches
            matches, details = scrape_recent_pro_matches(
                individual_files=not args.no_individual_files,
                api_key=args.api_key
            )
            if matches and details:
                analyze_data()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
