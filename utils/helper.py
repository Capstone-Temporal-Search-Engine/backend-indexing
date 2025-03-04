from datetime import datetime, timedelta

def get_months_between(start_timestamp, end_timestamp):
    start_date = datetime.utcfromtimestamp(start_timestamp)
    end_date = datetime.utcfromtimestamp(end_timestamp)

    months = []
    current_date = start_date.replace(day=1)  # Start from the first day of the month

    while current_date <= end_date:
        months.append(f'{current_date.month:02d}-{current_date.year}')  # Ensures two-digit month format
        # Move to the next month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)

    return months
