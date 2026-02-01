# hbase_implementation.py
"""
PART 1: HBase Implementation - Complete Package
Includes:
1. Schema design documentation
2. Data loading script
3. Query examples
4. Design justification
"""

import happybase
import json
from datetime import datetime
import struct

# ============================================================================
# HBASE SCHEMA DESIGN & JUSTIFICATION DOCUMENTATION
# ============================================================================

HBASE_DESIGN_DOC = """
================================================================================
HBASE SCHEMA DESIGN & JUSTIFICATION
================================================================================

DESIGN PHILOSOPHY:
-----------------
HBase excels at:
1. Time-series data with efficient time-range scans
2. Sparse data (not all columns need values)
3. High-volume writes and reads by row key
4. Sequential access patterns

This  leverages these strengths for e-commerce session and product analytics.

================================================================================
TABLE 1: USER_SESSIONS - Time-series User Browsing Data
================================================================================

ROW KEY DESIGN: user_id + reverse_timestamp
-------------------------------------------
Format: user_{user_id}_{reverse_timestamp}
Example: user_000042_9999999999999999

Justification:
- user_id prefix: Groups all sessions for a user together
- reverse_timestamp: Orders sessions newest-first (99999... - actual_timestamp)
  This allows efficient retrieval of recent sessions without scanning old data
- Combined key enables: "Get last N sessions for user X" with simple scan

Why Reverse Timestamp?
- HBase scans are sequential and efficient
- Most common query: "Show me user's recent activity"
- Reverse timestamp puts recent data at the beginning of user's row range
- Example: To get user_000042's last 10 sessions:
  SCAN with STARTROW='user_000042_', LIMIT=10

COLUMN FAMILIES:
---------------

1. session_info (Frequently accessed session metadata)
   - session_info:session_id        -> Original session ID
   - session_info:start_time        -> Session start timestamp
   - session_info:end_time          -> Session end timestamp
   - session_info:duration          -> Duration in seconds
   - session_info:referrer          -> Traffic source

2. device_info (Device and location context)
   - device_info:type               -> mobile/desktop/tablet
   - device_info:os                 -> Operating system
   - device_info:browser            -> Browser name
   - device_info:city               -> User's city
   - device_info:state              -> User's state
   - device_info:country            -> User's country
   - device_info:ip                 -> IP address

3. behavior (User actions and outcomes)
   - behavior:conversion_status     -> converted/abandoned/browsed
   - behavior:viewed_products       -> JSON array of product IDs
   - behavior:page_views            -> JSON array of page view objects
   - behavior:cart_contents         -> JSON array of cart items
   - behavior:products_viewed_count -> Count of unique products

Why These Column Families?
- session_info: Core session data, always queried together
- device_info: Context data, useful for device-based analytics
- behavior: User actions, may be queried separately for conversion analysis
- Separation allows selective column retrieval, reducing data transfer

QUERY PATTERNS SUPPORTED:
------------------------
1. Get all sessions for a user (recent first):
   scan 'user_sessions', {STARTROW => 'user_000042_', STOPROW => 'user_000042_~'}

2. Get last N sessions for a user:
   scan 'user_sessions', {STARTROW => 'user_000042_', LIMIT => 10}

3. Get sessions in a time range:
   scan 'user_sessions', {
     STARTROW => 'user_000042_9999999999999',
     STOPROW => 'user_000042_9000000000000'
   }

4. Get specific session data:
   get 'user_sessions', 'user_000042_9999999999999'

5. Get only behavioral data (selective column family):
   scan 'user_sessions', {
     STARTROW => 'user_000042_',
     COLUMNS => ['behavior']
   }

================================================================================
TABLE 2: PRODUCT_METRICS - Product Performance Over Time
================================================================================

ROW KEY DESIGN: product_id + date
----------------------------------
Format: prod_{product_id}_{YYYYMMDD}
Example: prod_00123_20260128

Justification:
- product_id prefix: Groups all metrics for a product together
- date suffix: Enables time-series analysis per product
- Natural ordering: Chronological within each product
- Supports queries like: "Show product X's performance over last 30 days"

COLUMN FAMILIES:
---------------

1. views (View and engagement metrics)
   - views:count                    -> Number of views this day
   - views:unique_users             -> Count of unique users who viewed
   - views:avg_view_duration        -> Average time spent viewing
   - views:sessions_with_view       -> Number of sessions that viewed product

2. sales (Transaction and revenue data)
   - sales:units_sold               -> Quantity sold this day
   - sales:revenue                  -> Total revenue generated
   - sales:num_transactions         -> Number of transactions
   - sales:avg_order_value          -> Average $ per transaction
   - sales:conversion_rate          -> Views to purchases ratio

3. inventory (Stock tracking)
   - inventory:stock_start          -> Stock at start of day
   - inventory:stock_end            -> Stock at end of day
   - inventory:restock_events       -> Count of restocking
   - inventory:stockout_duration    -> Minutes out of stock

Why These Column Families?
- views: Engagement metrics, queried for funnel analysis
- sales: Revenue metrics, queried for financial reporting
- inventory: Operational metrics, queried for supply chain
- Time-series aggregation: Easy to sum metrics across date ranges

QUERY PATTERNS SUPPORTED:
------------------------
1. Get all metrics for a product across all dates:
   scan 'product_metrics', {
     STARTROW => 'prod_00123_',
     STOPROW => 'prod_00123_~'
   }

2. Get product metrics for specific date:
   get 'product_metrics', 'prod_00123_20260128'

3. Get metrics for date range:
   scan 'product_metrics', {
     STARTROW => 'prod_00123_20260101',
     STOPROW => 'prod_00123_20260131'
   }

4. Get only sales data (selective column family):
   scan 'product_metrics', {
     STARTROW => 'prod_00123_',
     COLUMNS => ['sales']
   }

5. Aggregate product performance over time (in application logic):
   Scan date range, sum sales:revenue for total revenue

================================================================================
MONGODB vs HBASE: WHY CERTAIN DATA GOES WHERE
================================================================================

DATA IN MONGODB:
---------------
✓ Products catalog
  - Why: Complex nested structure (price_history array)
  - Why: Frequent updates to single documents
  - Why: Rich queries on product attributes

✓ Users profiles (with purchase_summary)
  - Why: Nested geo_data structure
  - Why: Embedded purchase summary for quick access
  - Why: Document model fits user profile naturally

✓ Transactions
  - Why: Embedded items array (line items belong to transaction)
  - Why: Complex financial data with discounts, totals
  - Why: Need ACID properties for payment processing

✓ Categories
  - Why: Hierarchical structure (embedded subcategories)
  - Why: Infrequent changes, frequent reads
  - Why: Small documents, perfect for document model

DATA IN HBASE:
--------------
✓ User sessions (time-series browsing)
  - Why: Time-series access pattern (recent sessions first)
  - Why: High write volume (millions of sessions)
  - Why: Sequential scan efficiency for user activity timeline
  - Why: Sparse data (not all sessions have all behaviors)

✓ Product metrics (daily aggregates)
  - Why: Time-series aggregation by date
  - Why: Efficient range scans for reporting
  - Why: Write-heavy (continuous metric updates)
  - Why: Column family structure fits metric categories

KEY DIFFERENCES:
---------------
MongoDB                          | HBase
--------------------------------|--------------------------------
Document-oriented                | Column-family oriented
Rich queries, indexes            | Row key scan-based queries
Nested structures                | Flat key-value with versions
Best for: Complex documents      | Best for: Time-series, high volume
Updates: Whole document          | Updates: Individual columns
Read pattern: Random access      | Read pattern: Sequential scans
Use case: Transactional data     | Use case: Analytical data streams

DESIGN TRADE-OFFS:
-----------------
✓ Data duplication: Session data in HBase + MongoDB (transactions reference sessions)
  - Trade-off: Storage vs Query Performance
  - Benefit: Each DB optimized for its query patterns

✓ No joins in HBase: Product details not stored with metrics
  - Trade-off: Must enrich in application layer
  - Benefit: HBase stays lean, fast for metrics

✓ Row key design: Fixed format limits flexibility
  - Trade-off: Query flexibility vs Scan efficiency
  - Benefit: Blazing fast time-range queries

================================================================================
"""

print(HBASE_DESIGN_DOC)

# ============================================================================
# DATA LOADING SCRIPT
# ============================================================================

print("\n" + "="*80)
print("HBASE DATA LOADING SCRIPT")
print("="*80)

# Connection to HBase (Docker)
print("\nConnecting to HBase...")
try:
    connection = happybase.Connection('localhost', port=9090)
    connection.open()
    print("✓ Connected to HBase successfully")
except Exception as e:
    print(f"✗ Connection failed: {e}")
    print("\nNote: Make sure Thrift server is running:")
    print("  docker exec -d hbase hbase thrift start -p 9090")
    exit(1)

# ============================================================================
# LOAD USER SESSIONS INTO HBASE
# ============================================================================

print("\n" + "="*80)
print("LOADING USER SESSIONS DATA")
print("="*80)

DATA_PATH = "/Users/honorinemnono/Desktop/BDA FINAL PROJECT/generateddata/"

def reverse_timestamp(timestamp_str):
    """
    Convert timestamp to reverse timestamp for newest-first ordering.
    """
    dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    # Convert to milliseconds since epoch
    ts_ms = int(dt.timestamp() * 1000)
    # Reverse: subtract from max value
    reverse_ts = 9999999999999 - ts_ms
    return str(reverse_ts).zfill(13)

def create_session_row_key(user_id, start_time):
    """
    Create HBase row key: user_id + reverse_timestamp
    """
    reverse_ts = reverse_timestamp(start_time)
    return f"{user_id}_{reverse_ts}"

# Get table
table = connection.table('user_sessions')

print("\nLoading session files...")

sessions_loaded = 0
batch_size = 1000
batch = table.batch()

# Load first session file (adjust number of files as needed)
for file_idx in range(1):  # Load sessions_0.json (100k sessions)
    try:
        filename = DATA_PATH + f'sessions_{file_idx}.json'
        print(f"\nProcessing {filename}...")
        
        with open(filename, 'r') as f:
            sessions = json.load(f)
        
        print(f"  Found {len(sessions)} sessions")
        
        # Load subset (first 10,000 for testing, adjust as needed)
        sessions_subset = sessions[:10000]
        
        for session in sessions_subset:
            row_key = create_session_row_key(session['user_id'], session['start_time'])
            
            # Prepare data for HBase
            data = {
                # session_info column family
                b'session_info:session_id': session['session_id'].encode(),
                b'session_info:start_time': session['start_time'].encode(),
                b'session_info:end_time': session['end_time'].encode(),
                b'session_info:duration': str(session['duration_seconds']).encode(),
                b'session_info:referrer': session['referrer'].encode(),
                
                # device_info column family
                b'device_info:type': session['device_profile']['type'].encode(),
                b'device_info:os': session['device_profile']['os'].encode(),
                b'device_info:browser': session['device_profile']['browser'].encode(),
                b'device_info:city': session['geo_data']['city'].encode(),
                b'device_info:state': session['geo_data']['state'].encode(),
                b'device_info:country': session['geo_data']['country'].encode(),
                b'device_info:ip': session['geo_data']['ip_address'].encode(),
                
                # behavior column family
                b'behavior:conversion_status': session['conversion_status'].encode(),
                b'behavior:viewed_products': json.dumps(session['viewed_products']).encode(),
                b'behavior:products_viewed_count': str(len(session['viewed_products'])).encode(),
                b'behavior:page_views': json.dumps(session['page_views']).encode(),
                b'behavior:cart_contents': json.dumps(session.get('cart_contents', {})).encode(),
            }
            
            batch.put(row_key.encode(), data)
            sessions_loaded += 1
            
            # Send batch every batch_size records
            if sessions_loaded % batch_size == 0:
                batch.send()
                batch = table.batch()
                print(f"  Loaded {sessions_loaded} sessions...")
        
        # Send remaining batch
        batch.send()
        print(f"✓ Completed {filename}: {len(sessions_subset)} sessions loaded")
        
    except FileNotFoundError:
        print(f"  {filename} not found, skipping...")
    except Exception as e:
        print(f"  Error loading {filename}: {e}")

print(f"\n✓ Total sessions loaded: {sessions_loaded}")

# ============================================================================
# QUERY EXAMPLES
# ============================================================================

print("\n" + "="*80)
print("HBASE QUERY EXAMPLES")
print("="*80)

print("\n1. Get last 5 sessions for user_000042:")
print("-" * 40)

# Note: These are example queries - actual execution requires proper setup
QUERY_EXAMPLES = """
# Query 1: Get last 5 sessions for a specific user
scan 'user_sessions', {
  STARTROW => 'user_000042_',
  STOPROW => 'user_000042_~',
  LIMIT => 5
}

# Query 2: Get all sessions for a user
scan 'user_sessions', {
  STARTROW => 'user_000042_',
  STOPROW => 'user_000042_~'
}

# Query 3: Get specific session details
get 'user_sessions', 'user_000042_9999999999999'

# Query 4: Get only behavioral data for user
scan 'user_sessions', {
  STARTROW => 'user_000042_',
  COLUMNS => ['behavior'],
  LIMIT => 10
}

# Query 5: Get sessions with conversion
scan 'user_sessions', {
  STARTROW => 'user_000042_',
  FILTER => "SingleColumnValueFilter('behavior', 'conversion_status', =, 'binary:converted')"
}
"""

print(QUERY_EXAMPLES)

# Python query example using happybase
print("\nPython Query Example:")
print("-" * 40)

try:
    # Get last 5 sessions for a user
    user_id = 'user_000001'
    start_row = f'{user_id}_'.encode()
    stop_row = f'{user_id}_~'.encode()
    
    print(f"\nQuerying sessions for {user_id}...")
    
    count = 0
    for key, data in table.scan(row_start=start_row, row_stop=stop_row, limit=5):
        count += 1
        row_key = key.decode()
        session_id = data[b'session_info:session_id'].decode()
        start_time = data[b'session_info:start_time'].decode()
        conversion = data[b'behavior:conversion_status'].decode()
        device = data[b'device_info:type'].decode()
        
        print(f"\n  Session {count}:")
        print(f"    Row Key: {row_key}")
        print(f"    Session ID: {session_id}")
        print(f"    Start Time: {start_time}")
        print(f"    Device: {device}")
        print(f"    Conversion: {conversion}")
    
    if count == 0:
        print(f"  No sessions found for {user_id}")
    else:
        print(f"\n✓ Retrieved {count} sessions")
        
except Exception as e:
    print(f"  Query error: {e}")

# Save query examples to file
with open('hbase_query_examples.txt', 'w') as f:
    f.write(QUERY_EXAMPLES)
    f.write("\n\n# Python Query Example\n")
    f.write("""
import happybase

connection = happybase.Connection('localhost', port=9090)
table = connection.table('user_sessions')

# Get last 5 sessions for user
user_id = 'user_000042'
start_row = f'{user_id}_'.encode()
stop_row = f'{user_id}_~'.encode()

for key, data in table.scan(row_start=start_row, row_stop=stop_row, limit=5):
    print(f"Session: {data[b'session_info:session_id'].decode()}")
    print(f"Time: {data[b'session_info:start_time'].decode()}")
    print(f"Device: {data[b'device_info:type'].decode()}")
    print(f"Converted: {data[b'behavior:conversion_status'].decode()}")
    print("-" * 40)

connection.close()
""")

print("\n✓ Query examples saved to hbase_query_examples.txt")

# Close connection
connection.close()

print("\n" + "="*80)
print("HBASE IMPLEMENTATION COMPLETE!")
print("="*80)
print("""
Deliverables Generated:
✓ Schema design with justification (printed above)
✓ Row key design explanation
✓ Column family design rationale
✓ Data loading script (loaded 10,000 sessions)
✓ Query examples (HBase shell + Python)
✓ MongoDB vs HBase comparison

Files created:
- hbase_query_examples.txt

Next Steps:
1. Include schema design in your technical report
2. Document the queries implemented
3. Explain why HBase for sessions vs MongoDB for transactions
""")