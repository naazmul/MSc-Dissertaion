import pandas as pd  # type: ignore
import pymysql  # type: ignore
import pymongo  # type: ignore
import time
import psutil  # type: ignore
from faker import Faker  # type: ignore
import random
import threading

# ------------------------
# Benchmark Result Tracker
# ------------------------

class BenchmarkResult:
    def __init__(self):
        self.results = []

    def add(self, operation, db, trial, value):
        self.results.append({
            "operation": operation,
            "database": db,
            "trial": trial,
            "value": round(value, 3)
        })

    def to_df(self):
        return pd.DataFrame(self.results)

    def summary(self):
        df = self.to_df()
        if df.empty:
            print("\n❌ No benchmark results to summarize.")
            return pd.DataFrame()

        summary = {}

        def show_table(title, filter_cond, metric_name):
            table = df[filter_cond].groupby(["operation", "database"]).agg(
                avg_value=("value", "mean"),
                min_value=("value", "min"),
                max_value=("value", "max"),
                runs=("value", "count")
            ).round(3).reset_index()
            print(f"\n✨ **{title}**")
            print(table.to_markdown(index=False))
            summary[metric_name] = table

        show_table("Execution Time (sec)", df["operation"].isin(["insert", "select", "update", "aggregate", "join"]), "timing")
        show_table("Storage Usage (MB)", df["operation"] == "storage_mb", "storage")
        show_table("Update Success (rows)", df["operation"] == "update_success", "updates")
        show_table("Query Accuracy (rows)", df["operation"] == "query_accuracy", "accuracy")
        show_table("CPU Usage (%)", df["operation"].str.endswith("_cpu"), "cpu")
        show_table("RAM Usage (MB)", df["operation"].str.endswith("_ram"), "ram")

        return summary

# --------------------
# DB CONNECTION HELPERS
# --------------------

def connect_mysql():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="nazmul.2025",
        database="benchmark_db"
    )

def connect_mongodb():
    return pymongo.MongoClient("mongodb://localhost:27017/")

# --------------------------
# UNIVERSAL METRIC DECORATOR
# --------------------------

def with_metrics(operation):
    def wrapper_func(func):
        def inner(*args, **kwargs):
            db_type = kwargs.get("db_type") or args[0]
            trial = kwargs.get("trial") or args[1]
            tracker = kwargs.get("tracker") or args[2]

            process = psutil.Process()
            mem_before = process.memory_info().rss / (1024 * 1024)
            start = time.time()

            # Variables to track peak RAM and CPU samples
            peak_ram = mem_before
            cpu_samples = []
            stop_polling = False

            # Background thread to poll resources during execution
            def poll_resources():
                while not stop_polling:
                    mem_now = process.memory_info().rss / (1024 * 1024)
                    nonlocal peak_ram
                    peak_ram = max(peak_ram, mem_now)
                    cpu = process.cpu_percent(interval=0.01) / psutil.cpu_count() # type: ignore
                    cpu_samples.append(cpu)
                    time.sleep(0.01)  # Poll every 10ms

            # Start polling in a separate thread
            polling_thread = threading.Thread(target=poll_resources)
            polling_thread.start()

            # Run the database operation
            result = func(*args, **kwargs)

            # Stop polling after operation completes
            stop_polling = True
            polling_thread.join()

            end = time.time()
            duration = end - start
            avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0
            ram_used = max(0, peak_ram - mem_before)

            tracker.add(operation, db_type, trial, duration)
            tracker.add(f"{operation}_cpu", db_type, trial, avg_cpu)
            tracker.add(f"{operation}_ram", db_type, trial, ram_used)

            print(f"\n⏱ {operation.upper()} on {db_type} (Trial {trial}):")
            print(f"   Time: {duration:.2f}s | CPU: {avg_cpu:.1f}% | RAM: {ram_used:.2f} MB")
            return result
        return inner
    return wrapper_func

# ------------------
# BENCHMARK OPERATIONS
# ------------------

@with_metrics("insert")
def run_insert(db_type, trial, tracker, df, drop_existing=False):
    if db_type == "MySQL":
        conn = connect_mysql()
        cursor = conn.cursor()
        if drop_existing:
            cursor.execute("DROP TABLE IF EXISTS orders")
            cursor.execute("DROP TABLE IF EXISTS customers")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS customers (
                    CustomerID VARCHAR(20) PRIMARY KEY,
                    Name VARCHAR(100)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    InvoiceNo VARCHAR(20),
                    StockCode VARCHAR(20),
                    Description TEXT,
                    Quantity INT,
                    InvoiceDate DATETIME,
                    UnitPrice FLOAT,
                    CustomerID VARCHAR(20),
                    Country VARCHAR(100),  -- Retain Country in orders
                    FOREIGN KEY (CustomerID) REFERENCES customers(CustomerID)
                )
            """)
            # Insert unique customers first (only CustomerID and Name)
            customers_df = df[["CustomerID"]].drop_duplicates().assign(Name=lambda x: "Cust_" + x["CustomerID"])
            customers_data = [tuple(row) for row in customers_df[["CustomerID", "Name"]].values]
            cursor.executemany("INSERT IGNORE INTO customers (CustomerID, Name) VALUES (%s, %s)", customers_data)
            conn.commit()
        # Insert orders (including Country)
        insert_query = """
            INSERT INTO orders 
            (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        for i in range(0, len(df), 1000):
            batch = df.iloc[i:i+1000]
            values = [tuple(x) for x in batch[["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"]].to_numpy()]
            cursor.executemany(insert_query, values)
            conn.commit()
        cursor.close()
        conn.close()
    elif db_type == "MongoDB":
        collection = connect_mongodb()["benchmark_db"]["orders"]
        if drop_existing:
            collection.drop()
        # Embed customer data in documents
        df["Name"] = "Cust_" + df["CustomerID"]
        collection.insert_many(df.to_dict("records"))

@with_metrics("select")
def run_query(db_type, trial, tracker, filter_country="United Kingdom"):
    start_date = pd.to_datetime("2010-12-01")
    if db_type == "MySQL":
        conn = connect_mysql()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM orders
            WHERE Country = %s AND InvoiceDate >= %s
        """, (filter_country, start_date))
        result = cursor.fetchall()
        tracker.add("query_accuracy", db_type, trial, len(result))
        cursor.close()
        conn.close()
    elif db_type == "MongoDB":
        collection = connect_mongodb()["benchmark_db"]["orders"]
        result = list(collection.find({
            "Country": filter_country,
            "InvoiceDate": { "$gte": start_date }
        }))
        tracker.add("query_accuracy", db_type, trial, len(result))

@with_metrics("update")
def run_update(db_type, trial, tracker):
    if db_type == "MySQL":
        conn = connect_mysql()
        cursor = conn.cursor()
        rows = cursor.execute("""
            UPDATE orders
            SET UnitPrice = UnitPrice * 1.1
            WHERE Quantity > 50
        """)
        tracker.add("update_success", db_type, trial, rows)
        conn.commit()
        cursor.close()
        conn.close()
    elif db_type == "MongoDB":
        collection = connect_mongodb()["benchmark_db"]["orders"]
        result = collection.update_many(
            {"Quantity": { "$gt": 50 }},
            {"$mul": { "UnitPrice": 1.1 }}
        )
        tracker.add("update_success", db_type, trial, result.modified_count)

@with_metrics("aggregate")
def run_aggregate(db_type, trial, tracker):
    if db_type == "MySQL":
        conn = connect_mysql()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT Country, SUM(Quantity) FROM orders GROUP BY Country
        """)
        _ = cursor.fetchall()
        cursor.close()
        conn.close()
    elif db_type == "MongoDB":
        collection = connect_mongodb()["benchmark_db"]["orders"]
        pipeline = [{ "$group": { "_id": "$Country", "total_qty": { "$sum": "$Quantity" } } }]
        _ = list(collection.aggregate(pipeline))

@with_metrics("join")
def run_join(db_type, trial, tracker, filter_country="United Kingdom"):
    if db_type == "MySQL":
        conn = connect_mysql()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT c.CustomerID, c.Name, SUM(o.Quantity * o.UnitPrice) as TotalSpent
            FROM orders o
            JOIN customers c ON o.CustomerID = c.CustomerID
            WHERE o.Country = %s  -- Use Country from orders
            GROUP BY c.CustomerID, c.Name
        """, (filter_country,))
        result = cursor.fetchall()
        tracker.add("join_accuracy", db_type, trial, len(result))
        cursor.close()
        conn.close()
    elif db_type == "MongoDB":
        collection = connect_mongodb()["benchmark_db"]["orders"]
        pipeline = [
            {"$match": {"Country": filter_country}},
            {"$group": {
                "_id": "$CustomerID",
                "Name": {"$first": "$Name"},
                "TotalSpent": {"$sum": {"$multiply": ["$Quantity", "$UnitPrice"]}}
            }}
        ]
        result = list(collection.aggregate(pipeline))
        tracker.add("join_accuracy", db_type, trial, len(result))

def check_storage(db_type, tracker):
    if db_type == "MySQL":
        conn = connect_mysql()
        cursor = conn.cursor()
        cursor.execute("SHOW TABLE STATUS LIKE 'orders'")
        row = cursor.fetchone()
        size_bytes = row[6] + row[8]  # type: ignore # Data + Index
        cursor.close()
        conn.close()
    elif db_type == "MongoDB":
        stats = connect_mongodb()["benchmark_db"].command("collstats", "orders")
        size_bytes = stats.get("size", 0) + stats.get("totalIndexSize", 0)
    else:
        return
    size_mb = size_bytes / (1024 * 1024)
    tracker.add("storage_mb", db_type, 1, size_mb)
    print(f"\n{db_type} STORAGE USED: {size_mb:.2f} MB")

def generate_fake_data(n_rows):
    fake = Faker()
    # Generate unique CustomerIDs using a counter
    customer_ids = [str(10000 + i) for i in range(n_rows)]
    random.shuffle(customer_ids)
    return pd.DataFrame([{
        "InvoiceNo": fake.uuid4()[:8],
        "StockCode": str(random.randint(10000, 99999)),
        "Description": fake.word(),
        "Quantity": random.randint(1, 100),
        "InvoiceDate": fake.date_time_this_decade(),
        "UnitPrice": round(random.uniform(1.0, 100.0), 2),
        "CustomerID": customer_ids[i],
        "Country": fake.country()
    } for i in range(n_rows)])

def run_scaling_test(db_type, tracker):
    for size in [100_000, 500_000, 1_000_000]:
        print(f"\nRunning scalability test: {db_type} with {size:,} rows")
        df = generate_fake_data(size)
        run_insert(db_type=db_type, trial=size, tracker=tracker, df=df, drop_existing=True)