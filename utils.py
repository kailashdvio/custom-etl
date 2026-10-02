from datetime import datetime, timedelta


def current_and_previous_date():
    ts = datetime.now()

    ts = ts.strftime("%Y-%m-%d %H:%M:%S")
    ts = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")

    ts_current = ts.replace(hour=0, minute=0, second=0)
    ts_previous = ts.replace(hour=0, minute=0, second=0) - timedelta(days=1)

    ts_current = ts_current.strftime("%Y-%m-%d %H:%M:%S")
    ts_previous = ts_previous.strftime("%Y-%m-%d %H:%M:%S")

    return ts_current, ts_previous
