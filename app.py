
import csv
import os
from datetime import datetime, timedelta
from flask import Flask, render_template, abort, request
import db
import threading

app = Flask(__name__)

CSV_FILE = 'access_counts.csv'
csv_lock = threading.Lock()

@app.before_request
def log_access_to_csv():

    if request.path.startswith('/static'):
        return

    client_ip = request.remote_addr

    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        client_ip = forwarded.split(',')[0].strip()

    if client_ip and client_ip[0] in ('=', '+', '-', '@', '\t', '\n', '\r'):
        client_ip = "'" + client_ip

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with csv_lock:
        data = {}

        if os.path.exists(CSV_FILE):
            try:
                with open(CSV_FILE, mode='r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        data[row['ip']] = {
                            'count': int(row['count']),
                            'last_access': row['last_access']
                        }
            except Exception as e:
                print(f"Error reading CSV: {e}")

        if client_ip in data:
            data[client_ip]['count'] += 1
            data[client_ip]['last_access'] = now
        else:
            data[client_ip] = {'count': 1, 'last_access': now}

        try:
            with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as f:
                fieldnames = ['ip', 'count', 'last_access']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for ip, info in data.items():
                    writer.writerow({
                        'ip': ip,
                        'count': info['count'],
                        'last_access': info['last_access']
                    })
        except Exception as e:
            print(f"Error writing CSV: {e}")

def adjust_to_brasilia(val):
    """
    Adjusts a UTC timestamp string (or date string) to Brasilia time (UTC-3).
    Handles both 'YYYY-MM-DD HH:MM:SS' and 'YYYY-MM-DD' formats.
    """
    if not val:
        return None

    dt = None
    try:

        dt = datetime.strptime(str(val), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:

            dt = datetime.strptime(str(val), "%Y-%m-%d")
        except ValueError:
            return val

    dt = dt - timedelta(hours=3)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def to_brasilia(value):
    """Converts UTC date/timestamp to Brasilia time (UTC-3)."""
    if not value:
        return "-"

    try:
        if isinstance(value, (int, float)):

            dt = datetime.utcfromtimestamp(value)
        elif isinstance(value, str):

            dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        else:
            return value

        dt = dt - timedelta(hours=3)
        return dt.strftime("%d/%m/%Y %H:%M")
    except ValueError:
        try:

             dt = datetime.strptime(value, "%Y-%m-%d %H:%M")
             dt = dt - timedelta(hours=3)
             return dt.strftime("%d/%m/%Y %H:%M")
        except:
             return value
    except Exception:
        return value

app.jinja_env.filters['brasilia_time'] = to_brasilia

@app.context_processor
def inject_last_sync():
    ts = db.get_last_sync_time()
    if ts:
        formatted_time = to_brasilia(ts)
    else:
        formatted_time = "Nunca"
    return dict(last_sync=formatted_time)

@app.route('/')
def index():

    stores = db.get_stores_with_all_cashbacks() 
    all_platforms = db.get_platforms()
    print(f"DEBUG: APP Index found {len(stores)} stores")

    return render_template('index.html', stores=stores, platforms=all_platforms)

@app.route('/store/<int:store_id>')
def store_details(store_id):
    data = db.get_store_details(store_id)
    if not data:
        abort(404)

    history_rows = db.get_cashback_history(store_id, None, None, None)
    history_data = []
    for row in history_rows:
        history_data.append({
            'date': adjust_to_brasilia(row['date_start']),
            'date_end': adjust_to_brasilia(row['date_end']),
            'value': row['value'],
            'value_specific': row['value_specific'],
            'description': row['description'],
            'platform': row['platform_name'],
            'platform_id': row['platform_id']
        })

    return render_template('store.html', store=data['store'], cashbacks=data['cashbacks'], history_data=history_data)

@app.route('/platforms')
def platforms():
    platforms_list = db.get_platforms()
    return render_template('platforms.html', platforms=platforms_list)

@app.route('/api/store/<int:store_id>/history')
def store_history(store_id):
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    if start_date:
        start_date += " 00:00:00"
    if end_date:
        end_date += " 23:59:59"

    platforms_str = request.args.get('platforms') 

    platform_ids = None
    if platforms_str:
        try:
            platform_ids = [int(p) for p in platforms_str.split(',')]
        except ValueError:
            pass 

    history_rows = db.get_cashback_history(store_id, start_date, end_date, platform_ids)

    data = []
    for row in history_rows:

        data.append({
            'date': adjust_to_brasilia(row['date_start']),
            'date_end': adjust_to_brasilia(row['date_end']),
            'value': row['value'],
            'value_specific': row['value_specific'],
            'description': row['description'],
            'platform': row['platform_name'],
            'platform_id': row['platform_id']
        })

    return {"history": data}

if __name__ == '__main__':

    app.run(host='0.0.0.0', port=80, debug=False)
