import csv
import os
import random
import time
from datetime import datetime

# 1. Settings
folder = "input_data"
os.makedirs(folder, exist_ok=True)

users = ["U001", "U002", "U003", "U004", "U005"]
products = ["P101", "P102", "P103", "P104"]
prices = {"P101": 15000, "P102": 4500, "P103": 32000, "P104": 800}

number_of_files = 10
events_per_file = 20

# A run code makes event IDs unique each time we run the generator
run_code = datetime.now().strftime("%H%M%S")
event_number = 1
total_rows = 0
previous_row = None

# 2. Make the files
for file_number in range(1, number_of_files + 1):
    rows = []

    for i in range(events_per_file):
        event_id = "E" + run_code + "-" + str(event_number)
        user_id = random.choice(users)
        product_id = random.choice(products)
        event_type = random.choice(["view", "view", "purchase"])
        event_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if event_type == "purchase":
            amount = prices[product_id]
        else:
            amount = ""

        rows.append([event_id, user_id, event_type, product_id, event_time, amount])
        event_number = event_number + 1

    # 3. Test data: every 3rd file gets one duplicate and one bad row
    if file_number % 3 == 0:
        rows.append(previous_row)                                    # duplicate event
        rows.append(["", "U001", "click", "P101", event_time, ""])   # bad row

    previous_row = rows[0]

    # 4. Write to a temporary name first, then rename
    #    (Spark ignores files starting with "_", so it never reads a half-written file)
    temp_name = folder + "/_writing.csv"
    final_name = folder + "/events_" + run_code + "_" + str(file_number) + ".csv"

    with open(temp_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["event_id", "user_id", "event_type", "product_id", "event_time", "amount"])
        writer.writerows(rows)

    os.rename(temp_name, final_name)
    total_rows = total_rows + len(rows)
    print("Created", final_name, "with", len(rows), "rows")

    time.sleep(5)  # wait 5 seconds, like real customers arriving over time

print("Done. Total rows generated:", total_rows)
