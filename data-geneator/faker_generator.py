# =============================================================================
#  faker_generator.py  —  THE DATA SOURCE  (creates fake banking data)
#  WHAT IT DOES: invents realistic customers, accounts and transactions and
#  INSERTs them into the Postgres "banking" database. Because Postgres has CDC
#  turned on, every insert is instantly captured by Debezium and streamed.
#
#  PIPELINE POSITION:  [THIS SCRIPT] --> Postgres --> Debezium/Kafka --> ...
#  Run it once:   python faker_generator.py --once
#  Run forever:   python faker_generator.py           (a new batch every 2s)
# =============================================================================

import time                                   # to sleep between loops
import psycopg2                               # the PostgreSQL driver (connect + run SQL)
from decimal import Decimal, ROUND_DOWN       # EXACT money math (never use float for money)
from faker import Faker                       # generates fake names / emails
import random                                 # random choices + amounts
import argparse                               # parse the --once command-line flag
import sys                                    # for a clean exit at the end
import os                                     # read env vars
from dotenv import load_dotenv                # load this folder's .env (DB credentials)

load_dotenv()   # pull POSTGRES_HOST/PORT/DB/USER/PASSWORD from data-geneator/.env

# -----------------------------------------------------------------------------
# Configuration — how much data each loop creates. Safe to hard-code (not secret).
# -----------------------------------------------------------------------------
NUM_CUSTOMERS = 10          # 10 customers per loop
ACCOUNTS_PER_CUSTOMER = 2   # each customer gets 2 accounts -> 20 accounts per loop
NUM_TRANSACTIONS = 50       # 50 transactions per loop
MAX_TXN_AMOUNT = 1000.00    # biggest transaction amount
CURRENCY = "USD"

INITIAL_BALANCE_MIN = Decimal("10.00")    # opening balances are between $10
INITIAL_BALANCE_MAX = Decimal("1000.00")  # and $1000 (Decimal = exact, for money)

DEFAULT_LOOP = True   # by default keep looping
SLEEP_SECONDS = 2     # wait 2 seconds between loops

# -----------------------------------------------------------------------------
# Command-line flag handling. "--once" lets you run a single batch and stop
# (great for testing), instead of the infinite loop.
# -----------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Run fake data generator")
parser.add_argument("--once", action="store_true", help="Run a single iteration and exit")
args = parser.parse_args()
LOOP = not args.once and DEFAULT_LOOP   # loop forever UNLESS --once was passed

fake = Faker()   # the fake-data generator object

# Make a random money amount with EXACTLY 2 decimal places, rounded down.
# Using Decimal (not float) avoids rounding errors that matter for money.
def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    val = Decimal(str(random.uniform(float(min_val), float(max_val))))   # random value
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)            # cut to 2 dp

# -----------------------------------------------------------------------------
# Connect to Postgres. Credentials come from .env (never hard-coded).
# -----------------------------------------------------------------------------
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),       # localhost (this script runs on your Mac)
    port=os.getenv("POSTGRES_PORT"),       # 5432
    dbname=os.getenv("POSTGRES_DB"),       # banking
    user=os.getenv("POSTGRES_USER"),       # postgres
    password=os.getenv("POSTGRES_PASSWORD"),
)
conn.autocommit = True   # commit each statement immediately -> Debezium sees it AT ONCE
cur = conn.cursor()      # the cursor is what we run SQL through

# -----------------------------------------------------------------------------
# One iteration = create 10 customers, their 20 accounts, then 50 transactions.
# ORDER MATTERS: customers first (accounts reference a customer; transactions
# reference an account). Inserting in the wrong order breaks the foreign keys.
# -----------------------------------------------------------------------------
def run_iteration():
    customers = []
    # 1) CUSTOMERS ------------------------------------------------------------
    for _ in range(NUM_CUSTOMERS):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.unique.email()        # .unique guarantees no repeat (matches UNIQUE column)
        cur.execute(
            "INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
            (first_name, last_name, email),   # %s placeholders prevent SQL injection
        )
        customer_id = cur.fetchone()[0]    # RETURNING id gives back the new primary key...
        customers.append(customer_id)      # ...which we keep to link accounts below

    # 2) ACCOUNTS -------------------------------------------------------------
    accounts = []
    for customer_id in customers:                 # for each customer we just made
        for _ in range(ACCOUNTS_PER_CUSTOMER):    # create 2 accounts
            account_type = random.choice(["SAVINGS", "CHECKING"])     # pick one at random
            initial_balance = random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX)
            cur.execute(
                "INSERT INTO accounts (customer_id, account_type, balance, currency) VALUES (%s, %s, %s, %s) RETURNING id",
                (customer_id, account_type, initial_balance, CURRENCY),
            )
            account_id = cur.fetchone()[0]   # keep each new account id...
            accounts.append(account_id)      # ...to link transactions below

    # 3) TRANSACTIONS ---------------------------------------------------------
    txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
    for _ in range(NUM_TRANSACTIONS):
        account_id = random.choice(accounts)       # the account doing the transaction
        txn_type = random.choice(txn_types)         # deposit / withdrawal / transfer
        amount = round(random.uniform(1, MAX_TXN_AMOUNT), 2)   # a random amount
        related_account = None                      # only used for transfers (sender->receiver)
        # For a TRANSFER, pick a DIFFERENT account as the receiver (can't transfer to yourself).
        if txn_type == "TRANSFER" and len(accounts) > 1:
            related_account = random.choice([a for a in accounts if a != account_id])
        cur.execute(
            "INSERT INTO transactions (account_id, txn_type, amount, related_account_id, status) VALUES (%s, %s, %s, %s, 'COMPLETED')",
            (account_id, txn_type, amount, related_account),
        )

    print(f"✅ Generated {len(customers)} customers, {len(accounts)} accounts, {NUM_TRANSACTIONS} transactions.")

# -----------------------------------------------------------------------------
# Main loop. try/except/finally makes sure we ALWAYS close the DB connection,
# even if you press Ctrl+C to stop it.
# -----------------------------------------------------------------------------
try:
    iteration = 0
    while True:
        iteration += 1
        print(f"\n--- Iteration {iteration} started ---")
        run_iteration()
        print(f"--- Iteration {iteration} finished ---")
        if not LOOP:        # if --once was passed, stop after one batch
            break
        time.sleep(SLEEP_SECONDS)   # otherwise wait 2s and go again

except KeyboardInterrupt:           # Ctrl+C -> stop cleanly instead of crashing
    print("\nInterrupted by user. Exiting gracefully...")

finally:                            # ALWAYS runs (success, error, or Ctrl+C)
    cur.close()                     # close the cursor
    conn.close()                    # close the DB connection (free the resource)
    sys.exit(0)                     # exit with code 0 = "success"
