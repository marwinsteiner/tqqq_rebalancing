"""
The purpose of this script is to run the TQQQ rebalancing strategy on tastytrade.
The overall flow of the script is as follows:
1. Authenticate -- get the authentication token from tastytrade.
2. Get the current position of TQQQ -- get the overall PnL, compare with fixed allocation amount.
3. If the position size is not equal to the fixed allocation amount, rebalance.
4. Store the new position size.
5. Send an email update to relevant email address with the overall position size and actions taken.
6. Schedule 1-5 to run on the last trading day of the month.
"""

# Imports
import pandas as pd
import numpy as np
import requests
import shelve
import pytz
import time
import schedule
import smtplib
import calendar

from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar
from config import settings
from typing import Literal
from pathlib import Path
from loguru import logger
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Set up a logging directory
log_dir = Path(__file__).parent / 'logs'
log_dir.mkdir(exist_ok=True)

# Create log file path with timestamp
log_file = log_dir / f"tastytrade_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Configure logger to write to both console and file
logger.add(log_file, rotation="1 day")

EnvironmentType = Literal['sandbox', 'production']  # create a type alias

# ENVIRONMENT toggles between sandbox (testing) and production (live trading)
ENVIRONMENT: EnvironmentType = 'sandbox'
logger.info(f'Using environment: {ENVIRONMENT}')

fixed_allocation = 2000  # dollar value of my fixed allocation to TQQQ.


# ----------------Authentication----------------

def get_session_token(environment: EnvironmentType):
    """
    Get or generate a session token based on the environment.

    Args:
        environment (str): The environment type ('sandbox' or 'production').

    Returns:
        str: The session token if found or generated, None if the request fails.

    Examples:
        session_token = get_session_token('sandbox')
    """
    with shelve.open(str(Path(settings.SESSION_SHELF_DIR) / 'session_data')) as db:
        session_token = db.get('session_token')
        token_expiry = db.get('token_expiry')

        # Check if we have a valid token that hasn't expired
        if session_token and token_expiry and datetime.now() < token_expiry:
            logger.success('Found existing session token.', extra={'session_token': session_token})
            logger.info(f'Existing session token will expire at {token_expiry}.')
            return session_token

    # If we get here, we either don't have a token or it's expired
    logger.warning('Session token expired or invalid, generating new session token...')
    if environment == 'sandbox':
        url = f"{settings.TASTY_SANDBOX_BASE_URL}/sessions"
        logger.info(f'Using environment:{environment} with base url: {url}')
        payload = {
            "login": settings.TASTY_SANDBOX.USERNAME,
            "password": settings.TASTY_SANDBOX.PASSWORD
        }
    else:
        url = f"{settings.TASTY_PRODUCTION_BASE_URL}/sessions"
        logger.info(f'Using environment:{environment} with base url: {url}')
        payload = {
            "login": settings.TASTY_PRODUCTION.USERNAME,
            "password": settings.TASTY_PRODUCTION.PASSWORD
        }
    logger.debug('Generated payload.')
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, json=payload, headers=headers)
    logger.info(f'Posted request: {response}')

    if response.status_code == 201:
        logger.success(f'Response status code: {response.status_code}. Received session token.')
        data = response.json()
        new_session_token = data['data']['session-token']
        new_token_expiry = datetime.now() + timedelta(hours=24)
        logger.debug(f'Saved new session token expiring at: {new_token_expiry}.')

        # Open a new shelf connection to store the token
        with shelve.open(str(Path(settings.SESSION_SHELF_DIR) / 'session_data')) as db:
            db['session_token'] = new_session_token
            db['token_expiry'] = new_token_expiry
            logger.success('Stored new session token and token expiry.')

        return new_session_token
    else:
        logger.error(f'Session token request failed with response code: {response.status_code}.')
        logger.debug(f'{response.text}')
        return None


# ----------------Get Position----------------

def get_position(session_token):
    """
    Retrieve TQQQ position information using the provided session token.

    Args:
        session_token (str): The session token for authentication.

    Returns:
        tuple: A tuple containing (quantity, unrealized_pnl, current_price) where:
            - quantity (float): The quantity of TQQQ held (positive for long, negative for short)
            - unrealized_pnl (float): The unrealized profit/loss on the position
            - current_price (float): The current market price of TQQQ
    
    Raises:
        None
    """
    positions = requests.get(f"{settings.TASTY_SANDBOX_BASE_URL if ENVIRONMENT == 'sandbox' else settings.TASTY_PRODUCTION_BASE_URL}/accounts/{settings.TASTY_SANDBOX.ACCOUNT_NUMBER if ENVIRONMENT == 'sandbox' else settings.TASTY_PRODUCTION.ACCOUNT_NUMBER}/positions", 
    headers={'Authorization': session_token}).json()
    
    # Find TQQQ position in the items list
    tqqq_position = None
    for position in positions["data"]["items"]:
        if position["symbol"] == "TQQQ":
            tqqq_position = position
            break
    
    if tqqq_position is None:
        return 0, 0.0, 0.0  # Return 0 quantity, 0 PnL, and 0 price if no TQQQ position is found
    
    # Calculate quantity (positive for long, negative for short)
    quantity = float(tqqq_position["quantity"])
    if tqqq_position["quantity-direction"] == "Short":
        quantity = -quantity
    
    # Get current price and calculate unrealized PnL
    avg_open_price = float(tqqq_position["average-open-price"])
    close_price = float(tqqq_position["close-price"])
    multiplier = float(tqqq_position["multiplier"])
    unrealized_pnl = (close_price - avg_open_price) * abs(quantity) * multiplier
    
    # For short positions, reverse the PnL calculation
    if quantity < 0:
        unrealized_pnl = -unrealized_pnl
    
    return quantity, unrealized_pnl, close_price


# ----------------Rebalancer----------------
def rebalance(session_token):
    """
    Rebalance the TQQQ position to maintain the fixed allocation value.
    If the position value exceeds the fixed allocation, sell shares.
    If the position value is below the fixed allocation, buy shares.

    Args:
        session_token (str): The session token for authentication.

    Returns:
        tuple: A tuple containing (action, shares_to_trade) where:
            - action (str): 'BUY' or 'SELL'
            - shares_to_trade (int): Number of shares to trade
    """
    # Get current position information
    quantity, _, current_price = get_position(session_token)
    
    # Calculate current position value
    current_value = abs(quantity) * current_price
    
    # Calculate the difference between current value and target allocation
    value_difference = fixed_allocation - current_value
    
    # Calculate number of shares to trade (round down to nearest whole share)
    shares_to_trade = int(abs(value_difference) / current_price)
    
    if shares_to_trade == 0:
        return None, 0  # No trade needed
    
    # Determine if we need to buy or sell
    if value_difference > 0:
        return 'BUY', shares_to_trade
    else:
        return 'SELL', shares_to_trade


# ----------------Execute Order----------------
def execute_order(session_token, action, quantity):
    """
    Execute a limit order for TQQQ.

    Args:
        session_token (str): The session token for authentication
        action (str): 'BUY' or 'SELL'
        quantity (int): Number of shares to trade

    Returns:
        dict: The order response from the API
    """
    # Get current price to set limit price
    _, _, current_price = get_position(session_token)
    
    # Set limit price 0.5% away from current price
    # For buys: limit is 0.5% above current price
    # For sells: limit is 0.5% below current price
    price_adjustment = 1.005 if action == "BUY" else 0.995
    limit_price = round(current_price * price_adjustment, 2)
    
    # Convert our action to Tastyworks format
    tasty_action = "Buy to Open" if action == "BUY" else "Sell to Close"
    
    # Set price effect (Debit for buys, Credit for sells)
    price_effect = "Debit" if action == "BUY" else "Credit"
    
    order_payload = {
        "time-in-force": "Day",
        "order-type": "Limit",
        "price": limit_price,
        "price-effect": price_effect,
        "legs": [
            {
                "instrument-type": "Equity",
                "symbol": "TQQQ",
                "quantity": quantity,
                "action": tasty_action
            }
        ]
    }
    
    # Submit the order
    response = requests.post(
        f"{settings.TASTY_SANDBOX_BASE_URL if ENVIRONMENT == 'sandbox' else settings.TASTY_PRODUCTION_BASE_URL}/accounts/{settings.TASTY_SANDBOX.ACCOUNT_NUMBER if ENVIRONMENT == 'sandbox' else settings.TASTY_PRODUCTION.ACCOUNT_NUMBER}/orders",
        headers={'Authorization': session_token},
        json=order_payload
    )
    
    return response.json()


# ----------------Email Update----------------
def send_email_update(trade_info=None, error_message=None):
    """
    Send an email update about the TQQQ rebalancing activity.
    
    Args:
        trade_info (dict): Information about the trade executed
        error_message (str): Error message if something went wrong
    """
    try:
        # Create SMTP session
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(settings.EMAIL.SENDER, settings.EMAIL.SENDER_PASSWORD)
        
        # Get current position information
        session_token = get_session_token(ENVIRONMENT)
        quantity, unrealized_pnl, current_price = get_position(session_token)
        current_value = abs(quantity) * current_price
        
        # Construct email subject
        subject = "TQQQ Rebalancing Update"
        if error_message:
            subject += " - ERROR"
        
        # Construct email body
        body = []
        body.append(f"TQQQ Position Update - {datetime.now().strftime('%Y-%m-%d %H:%M:%S EST')}\n")
        body.append(f"Current Position:")
        body.append(f"  Shares: {quantity}")
        body.append(f"  Price: ${current_price:.2f}")
        body.append(f"  Position Value: ${current_value:.2f}")
        body.append(f"  Unrealized P&L: ${unrealized_pnl:.2f}")
        body.append(f"  Target Allocation: ${fixed_allocation:.2f}")
        body.append(f"  Difference from Target: ${current_value - fixed_allocation:.2f}")
        
        if trade_info:
            body.append(f"\nTrade Executed:")
            body.append(f"  Action: {trade_info['action']}")
            body.append(f"  Shares: {trade_info['shares']}")
            body.append(f"  Limit Price: ${trade_info['limit_price']:.2f}")
            
        if error_message:
            body.append(f"\nERROR:")
            body.append(f"  {error_message}")
            
        body.append(f"\nEnvironment: {ENVIRONMENT}")
        
        # Format email
        email_text = f"Subject: {subject}\n\n" + "\n".join(body)
        
        # Send email
        server.sendmail(settings.EMAIL.SENDER, settings.EMAIL.RECEIVER, email_text)
        server.quit()
        logger.info("Email update sent successfully")
        
    except Exception as e:
        logger.error(f"Failed to send email update: {str(e)}")


# ----------------Main----------------
def main():
    """
    Main function to run the TQQQ rebalancing strategy.
    """
    logger.info("Starting TQQQ rebalancing process...")
    
    try:
        # Get session token
        session_token = get_session_token(ENVIRONMENT)
        if not session_token:
            error_msg = "Failed to get session token"
            logger.error(error_msg)
            send_email_update(error_message=error_msg)
            return
        
        # Check if rebalancing is needed
        action, shares_to_trade = rebalance(session_token)
        
        if not action:
            logger.info("No rebalancing needed")
            send_email_update()  # Send position update even when no trade needed
            return
        
        # Execute the trade if needed
        logger.info(f"Executing {action} order for {shares_to_trade} shares of TQQQ")
        try:
            order_response = execute_order(session_token, action, shares_to_trade)
            
            # Prepare trade info for email
            trade_info = {
                'action': action,
                'shares': shares_to_trade,
                'limit_price': order_response.get('price', 0.0)
            }
            
            logger.info(f"Order submitted successfully: {order_response}")
            send_email_update(trade_info=trade_info)
            
        except Exception as e:
            error_msg = f"Error executing order: {str(e)}"
            logger.error(error_msg)
            send_email_update(error_message=error_msg)
            return

    except Exception as e:
        error_msg = f"Unexpected error in rebalancing process: {str(e)}"
        logger.error(error_msg)
        send_email_update(error_message=error_msg)
        return

    logger.info("TQQQ rebalancing completed")


def is_last_trading_day():
    """
    Check if today is the last trading day of the month using NYSE calendar.
    Returns True if it's the last trading day of the month, accounting for holidays.
    """
    nyse = get_calendar('NYSE')
    today = datetime.now().date()
    
    # Get the last day of the current month
    _, last_day = calendar.monthrange(today.year, today.month)
    month_end = datetime(today.year, today.month, last_day).date()
    
    # Get next month's first day
    if today.month == 12:
        next_month = datetime(today.year + 1, 1, 1).date()
    else:
        next_month = datetime(today.year, today.month + 1, 1).date()
    
    # Get the trading days for this month
    trading_days = nyse.valid_days(start_date=today, end_date=next_month)
    
    # Convert to date objects for comparison
    trading_days = [d.date() for d in trading_days]
    
    # If today is a trading day and it's the last one before next month
    return today in trading_days and today == trading_days[-1]


def scheduled_job():
    """
    Wrapper function that only runs main() if it's the last trading day of the month.
    """
    if is_last_trading_day():
        logger.info("Today is the last trading day of the month. Running rebalancing...")
        main()
    else:
        logger.info("Not the last trading day of the month. Skipping rebalancing.")


if __name__ == "__main__":
    logger.info("Starting TQQQ monthly rebalancing scheduler...")
    
    # Schedule the job to run every day at 15:45 EST (market close is 16:00 EST)
    schedule.every().day.at("15:45").do(scheduled_job)
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute