from apscheduler.schedulers.blocking import BlockingScheduler
from processors.conditions_processor import process_conditions
from processors.procedures_processor import process_procedures
from processors.observations_processor import process_observations
from processors.test import process_test
import logging
from logging import getLogger

# Ensure logging is set up
import logging
from logging import getLogger
import logging

# Set up logging configuration
from logging import setup_logging
setup_logging()

# Now you can use the logger
logger = getLogger(__name__)

def main():
    logger.info('Starting main job...')
    
    try:
        process_conditions()
        process_procedures()
        process_observations()
        process_test()
        logger.info('All processes completed successfully.')
    except Exception as e:
        logger.error(f'An error occurred: {e}')

if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'cron', hour=0, minute=0)
    logger.info('Scheduler started.')
    scheduler.start()
