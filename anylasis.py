import pandas as pd

# Load Excel
df = pd.read_excel("wic_data.xlsx")

# Convert all time columns to datetime
time_columns = ['Arrival Time', 'Front Desk Time', 'Staff Time', 'Departure Time']
for col in time_columns:
    df[col] = pd.to_datetime(df[col])

# Total appointment duration
df['Appointment Duration'] = (df['Departure Time'] - df['Arrival Time']).dt.total_seconds() / 60  # in minutes

# Front Desk Wait
df['Wait for Front Desk'] = (df['Front Desk Time'] - df['Arrival Time']).dt.total_seconds() / 60

# Wait for Staff
df['Wait for Staff'] = (df['Staff Time'] - df['Front Desk Time']).dt.total_seconds() / 60

# Staff Time Duration
df['Time with Staff'] = (df['Departure Time'] - df['Staff Time']).dt.total_seconds() / 60

# Avg appointment time overall
overall_avg = df['Appointment Duration'].mean()

# Avg time with/without interpreter
with_interp = df[df['Interpreter Needed'] == 'Yes']['Appointment Duration'].mean()
without_interp = df[df['Interpreter Needed'] == 'No']['Appointment Duration'].mean()

# Busy days/times
df['Day of Week'] = df['Date'].dt.day_name()
busy_day = df['Day of Week'].value_counts()

# Optional: hour of arrival
df['Hour of Arrival'] = df['Arrival Time'].dt.hour
busy_hours = df['Hour of Arrival'].value_counts().sort_index()

# Output summary
print("Average Appointment Duration (min):", overall_avg)
print("With Interpreter:", with_interp)
print("Without Interpreter:", without_interp)
print("\nBusy Days:\n", busy_day)
print("\nBusy Hours:\n", busy_hours)