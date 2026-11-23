"""Data generator: a pretend online shop that creates e-commerce events as CSV files.

Run examples (in the terminal):
  python data_generator.py                                   -> 10 files x 20 events, 5 seconds apart
  python data_generator.py --files 100 --events 1000 --delay 0 --seed 42
                                                             -> high volume: 100,000 events, as fast as possible
  python data_generator.py --help                            -> shows all the settings
"""
import argparse
import csv
import os
import random
import time
from datetime import datetime, timedelta

# 1. Settings you can change from the command line (no need to edit the code)
parser = argparse.ArgumentParser(description="Generate e-commerce event files for Spark")
parser.add_argument("--files", type=int, default=10, help="how many CSV files to create")
parser.add_argument("--events", type=int, default=20, help="how many events in each file")
parser.add_argument("--delay", type=float, default=5, help="seconds to wait between files (0 = no wait)")
parser.add_argument("--seed", type=int, default=None, help="same seed = same data every run (for testing)")
parser.add_argument("--bad-percent", type=float, default=2, help="% of rows that are bad on purpose")
parser.add_argument("--duplicate-percent", type=float, default=2, help="% of rows that are duplicates on purpose")
parser.add_argument("--late-percent", type=float, default=5, help="% of rows with an old event_time (late data)")
parser.add_argument("--folder", default="input_data", help="folder where Spark reads the files")
settings = parser.parse_args()

if settings.seed is not None:
    random.seed(settings.seed)

os.makedirs(settings.folder, exist_ok=True)

# 2. The shop: products (category and price in RWF) and customers
products = {
    "P101": ("electronics", 15000),
    "P102": ("electronics", 32000),
    "P103": ("fashion", 4500),
    "P104": ("fashion", 9000),
    "P105": ("food", 800),
    "P106": ("food", 1500),
    "P107": ("home", 12000),
    "P108": ("home", 2500),
    "P109": ("books", 3000),
    "P110": ("books", 6000),
}
product_ids = list(products.keys())
users = ["U" + str(number).zfill(4) for number in range(1, 201)]   # U0001 ... U0200

# Real shops: most people only look, fewer add to cart, even fewer buy
event_types = ["view", "add_to_cart", "purchase"]
event_weights = [70, 20, 10]

header = ["event_id", "user_id", "event_type", "product_id", "category", "price", "quantity", "event_time"]


# 3. Make one normal event
def make_event(event_id):
    user_id = random.choice(users)
    product_id = random.choice(product_ids)
    category, price = products[product_id]
    event_type = random.choices(event_types, weights=event_weights)[0]

    if event_type == "view":
        quantity = 1
    else:
        quantity = random.randint(1, 3)

    event_time = datetime.now()

    # Late data: some events arrive with an old time (e.g. the phone was offline)
    if random.random() * 100 < settings.late_percent:
        event_time = event_time - timedelta(minutes=random.randint(1, 15))

    return [event_id, user_id, event_type, product_id, category, price, quantity,
            event_time.strftime("%Y-%m-%d %H:%M:%S")]


# 4. Make one bad event on purpose (to test that Spark rejects it)
def make_bad_event(event_id):
    row = make_event(event_id)
    problem = random.choice(["no_event_id", "unknown_type", "negative_price", "bad_time", "no_user"])

    if problem == "no_event_id":
        row[0] = ""
    elif problem == "unknown_type":
        row[2] = "click"
    elif problem == "negative_price":
        row[5] = -500
    elif problem == "bad_time":
        row[7] = "not-a-date"
    elif problem == "no_user":
        row[1] = ""
    return row


# 5. Make the files
run_code = datetime.now().strftime("%Y%m%d%H%M%S")   # makes event IDs unique for each run
event_number = 1
all_rows = []          # remember rows so we can copy one as a duplicate
total_rows = 0
total_bad = 0
total_duplicates = 0
start_time = time.time()

for file_number in range(1, settings.files + 1):
    rows = []
    bad_in_file = 0
    duplicates_in_file = 0

    for i in range(settings.events):
        event_id = "E" + run_code + "-" + str(event_number)
        event_number = event_number + 1
        chance = random.random() * 100

        if chance < settings.bad_percent:
            rows.append(make_bad_event(event_id))
            bad_in_file = bad_in_file + 1
        elif chance < settings.bad_percent + settings.duplicate_percent and len(all_rows) > 0:
            rows.append(random.choice(all_rows))       # an event we already sent
            duplicates_in_file = duplicates_in_file + 1
        else:
            row = make_event(event_id)
            rows.append(row)
            all_rows.append(row)

    # Write to a temporary name first, then rename
    # (Spark ignores files starting with "_", so it never reads a half-written file)
    temp_name = os.path.join(settings.folder, "_writing.csv")
    final_name = os.path.join(settings.folder, "events_" + run_code + "_" + str(file_number) + ".csv")

    with open(temp_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    os.rename(temp_name, final_name)

    # Answer key: what we put in each file, so we can check Spark's results later
    log_exists = os.path.exists("generator_log.csv")
    with open("generator_log.csv", "a", newline="") as log:
        log_writer = csv.writer(log)
        if not log_exists:
            log_writer.writerow(["file", "rows", "bad_rows", "duplicate_rows", "created_at"])
        log_writer.writerow([os.path.basename(final_name), len(rows), bad_in_file, duplicates_in_file,
                             datetime.now().strftime("%Y-%m-%d %H:%M:%S")])

    total_rows = total_rows + len(rows)
    total_bad = total_bad + bad_in_file
    total_duplicates = total_duplicates + duplicates_in_file
    print("Created", final_name, "with", len(rows), "rows (bad:", bad_in_file, "| duplicates:", duplicates_in_file, ")")

    if settings.delay > 0 and file_number < settings.files:
        time.sleep(settings.delay)

seconds = time.time() - start_time
print()
print("Done. Total rows:", total_rows, "| bad:", total_bad, "| duplicates:", total_duplicates)
if seconds > 0:
    print("Time:", round(seconds, 2), "seconds | speed:", round(total_rows / seconds), "events per second")
