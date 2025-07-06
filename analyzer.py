import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import xlsxwriter
from collections import Counter

class WICClinicAnalyzerMonthly:
    # This class handles all the WIC clinic data from a monthly Excel file
    # It reads multiple daily sheets and combines them into one big analysis

    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file
        self.all_data = pd.DataFrame()
        self.sheet_names = []

    def is_interpreter_needed(self, language, comments):
        # Check if an appointment needs an interpreter
        # Look at both the language field and any comments about translation
        lang_check = isinstance(language, str) and language.strip().lower() not in ["english", "eng", "none", ""]
        comment_check = isinstance(comments, str) and any(
            kw in comments.lower() for kw in ["interpreter", "non-english", "translation"]
        )
        return lang_check or comment_check

    def load_and_process_all_sheets(self):
        # This is the main function that loads all the daily sheets and cleans them up
        print(f"Loading workbook: {self.input_file}...")
        excel_file = pd.ExcelFile(self.input_file)
        # Skip the 'totals' sheet since we don't need it
        self.sheet_names = [s for s in excel_file.sheet_names if s.lower() != 'totals']

        print(f"Found sheets: {self.sheet_names}")

        processed_frames = []

        for sheet in self.sheet_names:
            print(f"\nProcessing sheet: {sheet}...")
            df = pd.read_excel(self.input_file, sheet_name=sheet)
            
            # Skip sheets that don't have enough data
            if df.shape[0] < 3:
                print(f"  Skipping {sheet} - too few rows ({df.shape[0]})")
                continue
                
            df['sheet_date'] = sheet
            
            # Find all the time columns - these are the appointment timestamps
            datetime_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
            print(f"  Found {len(datetime_cols)} datetime columns: {datetime_cols}")
            
            if len(datetime_cols) < 2:
                print(f"  Skipping {sheet} - not enough datetime columns")
                continue

            # Map the time columns to what they actually represent
            # Usually goes: front desk -> intake -> peer -> health professional -> finish
            column_mapping = {}
            if len(datetime_cols) >= 1: column_mapping['FRONT DESK'] = datetime_cols[0]
            if len(datetime_cols) >= 2: column_mapping['INTAKE UP'] = datetime_cols[1]
            if len(datetime_cols) >= 3: column_mapping['PEER UP'] = datetime_cols[2]
            if len(datetime_cols) >= 4: column_mapping['HP UP'] = datetime_cols[3]
            if len(datetime_cols) >= 5: column_mapping['Finish Time - @ end of HP'] = datetime_cols[4]
            
            # If we have fewer columns, at least get the start and end times
            if len(datetime_cols) >= 2:
                column_mapping['FRONT DESK'] = datetime_cols[0]
                column_mapping['Finish Time - @ end of HP'] = datetime_cols[-1]

            # Copy the time columns with better names
            for logical_name, actual_col in column_mapping.items():
                df[logical_name] = df[actual_col]

            # Remove appointments that somehow got mixed up between different days
            expected_date = self._parse_sheet_date(sheet)
            if expected_date:
                print(f"  Expected date for sheet {sheet}: {expected_date}")
                df = self._filter_cross_date_contamination(df, expected_date, datetime_cols)

            # Try to find the language and comments columns by looking at the data
            language_col = None
            comments_col = None
            
            for col in df.columns:
                if df[col].dtype == 'object':  # Text columns only
                    sample_values = df[col].dropna().astype(str).str.lower()
                    if len(sample_values) > 0:
                        # Look for language-related words
                        if any(lang in ' '.join(sample_values) for lang in ['english', 'spanish', 'arabic', 'french']):
                            language_col = col
                            df['language'] = df[col]
                        # Look for comment-related words
                        elif any(word in ' '.join(sample_values) for word in ['comment', 'note', 'interpreter']):
                            comments_col = col
                            df['Comments'] = df[col]

            # Calculate how long each appointment took
            if 'FRONT DESK' in df.columns and 'Finish Time - @ end of HP' in df.columns:
                start_times = pd.to_datetime(df['FRONT DESK'], errors='coerce')
                end_times = pd.to_datetime(df['Finish Time - @ end of HP'], errors='coerce')
                
                # Get the time difference in minutes
                raw_durations = (end_times - start_times).dt.total_seconds() / 60
                
                # Filter out unrealistic appointment times
                # Has to be positive, less than 8 hours, and more than 5 minutes
                valid_mask = (
                    (raw_durations > 5) & 
                    (raw_durations < 480) & 
                    (raw_durations.notna())
                )
                
                df['appointment_duration_min'] = np.where(valid_mask, raw_durations, np.nan)
                
                # Show how many records we kept vs threw out
                total_records = len(df)
                valid_records = valid_mask.sum()
                print(f"  Duration filtering: {valid_records}/{total_records} records kept")
                if valid_records < total_records:
                    invalid_durations = raw_durations[~valid_mask].dropna()
                    if len(invalid_durations) > 0:
                        print(f"  Filtered out durations: min={invalid_durations.min():.1f}, max={invalid_durations.max():.1f}")
            else:
                df['appointment_duration_min'] = np.nan

            # Calculate time between each step in the process
            transition_cols = ['FRONT DESK', 'INTAKE UP', 'PEER UP', 'HP UP']
            for i in range(len(transition_cols) - 1):
                if transition_cols[i] in df.columns and transition_cols[i + 1] in df.columns:
                    start_col = transition_cols[i]
                    end_col = transition_cols[i + 1]
                    transition_name = f"transition_{start_col.lower().replace(' ', '_')}_to_{end_col.lower().replace(' ', '_')}"
                    
                    start_times = pd.to_datetime(df[start_col], errors='coerce')
                    end_times = pd.to_datetime(df[end_col], errors='coerce')
                    transition_duration = (end_times - start_times).dt.total_seconds() / 60
                    
                    # Keep transition times that make sense (0-2 hours)
                    valid_transitions = (transition_duration >= 0) & (transition_duration <= 120)
                    df[transition_name] = np.where(valid_transitions, transition_duration, np.nan)

            # Figure out which appointments needed an interpreter
            df["interpreter_needed"] = df.apply(
                lambda row: self.is_interpreter_needed(row.get("language", ""), row.get("Comments", "")), axis=1
            )

            # Add some useful time info like day of week and hour
            if 'FRONT DESK' in df.columns:
                appointment_times = pd.to_datetime(df['FRONT DESK'], errors='coerce')
                df['day_of_week'] = appointment_times.dt.day_name()
                df['hour_of_day'] = appointment_times.dt.hour
            else:
                df['day_of_week'] = np.nan
                df['hour_of_day'] = np.nan

            # Make sure all sheets have the same columns in the same order
            # This prevents issues when we combine everything later
            df.rename(columns={
                'sheet_date': 'date',
                'FRONT DESK': 'start_time',
                'Finish Time - @ end of HP': 'end_time'
            }, inplace=True)

            # Set up the final column structure
            expected_columns = [
                "date", "start_time", "end_time", "appointment_duration_min",
                "interpreter_needed", "day_of_week", "hour_of_day"
            ]
            df = df.reindex(columns=expected_columns)

            # Double-check that we didn't mess up the column order
            if list(df.columns) != expected_columns:
                raise ValueError(f"Column misalignment detected in sheet {sheet} — check sheet format before appending.")

            # Only keep rows that have valid appointment times
            valid_rows = df['appointment_duration_min'].notna()
            df_filtered = df[valid_rows].copy()
            
            print(f"  Final records for {sheet}: {len(df_filtered)}")
            if len(df_filtered) > 0:
                processed_frames.append(df_filtered)

        # Combine all the daily sheets into one big dataset
        if processed_frames:
            self.all_data = pd.concat(processed_frames, ignore_index=True)
            print(f"\nAll sheets processed! Total valid records: {len(self.all_data)}")
        else:
            print("\nWARNING: No valid data found in any sheets!")
            self.all_data = pd.DataFrame()

    def _parse_sheet_date(self, sheet_name):
        # Try to figure out what date a sheet represents from its name
        try:
            # Handle sheet names like '2522 0101' or just '0102'
            if sheet_name.startswith('2522'):
                date_part = sheet_name.split()[-1]  # Get last part
            else:
                date_part = sheet_name
            
            # Assume it's 2025 for now (change this if needed)
            month = int(date_part[:2])
            day = int(date_part[2:])
            year = 2025  # Adjust based on your data
            
            return datetime(year, month, day).date()
        except:
            return None

    def _filter_cross_date_contamination(self, df, expected_date, datetime_cols):
        # Remove appointments that somehow got the wrong date
        filtered_df = df.copy()
        
        for col in datetime_cols:
            if col in filtered_df.columns:
                times = pd.to_datetime(filtered_df[col], errors='coerce')
                # Keep only times that are within 1 day of what we expect
                valid_dates = times.dt.date
                date_mask = (
                    (valid_dates >= expected_date - timedelta(days=1)) & 
                    (valid_dates <= expected_date + timedelta(days=1))
                )
                filtered_df.loc[~date_mask, col] = pd.NaT
        
        return filtered_df

    def calculate_statistics(self):
        # Calculate all the summary stats for the whole month
        if self.all_data.empty:
            return {}, {}
            
        stats = {}
        busiest = {}

        # Basic duration statistics
        valid_durations = self.all_data['appointment_duration_min'].dropna()
        if not valid_durations.empty:
            stats['Total Valid Appointments'] = len(valid_durations)
            stats['Average Appointment Duration (minutes)'] = valid_durations.mean()
            stats['Median Appointment Duration (minutes)'] = valid_durations.median()
            stats['Max Appointment Duration (minutes)'] = valid_durations.max()
            stats['Min Appointment Duration (minutes)'] = valid_durations.min()
            stats['Std Dev Appointment Duration (minutes)'] = valid_durations.std()

        # Compare appointments with and without interpreters
        interpreter = self.all_data[self.all_data['interpreter_needed'] == True]
        no_interpreter = self.all_data[self.all_data['interpreter_needed'] == False]
        
        if not interpreter.empty and not no_interpreter.empty:
            interp_duration = interpreter['appointment_duration_min'].dropna()
            no_interp_duration = no_interpreter['appointment_duration_min'].dropna()
            
            if not interp_duration.empty:
                stats['Avg Duration WITH Interpreter (minutes)'] = interp_duration.mean()
                stats['Count WITH Interpreter'] = len(interp_duration)
            if not no_interp_duration.empty:
                stats['Avg Duration WITHOUT Interpreter (minutes)'] = no_interp_duration.mean()
                stats['Count WITHOUT Interpreter'] = len(no_interp_duration)

        # Find the busiest days and hours
        day_counts = self.all_data['day_of_week'].value_counts()
        if not day_counts.empty:
            busiest['busiest_days'] = day_counts.to_dict()
            stats['Busiest Day of Week'] = day_counts.idxmax()

        hour_counts = self.all_data['hour_of_day'].value_counts().sort_index()
        if not hour_counts.empty:
            busiest['busiest_hours'] = hour_counts.to_dict()
            stats['Busiest Hour of Day'] = hour_counts.idxmax()

        return stats, busiest

    def perform_qa_verification(self):
        # Print some key numbers so we can double-check against Excel manually
        if self.all_data.empty:
            print("No data available for QA verification.")
            return
        
        print("\n" + "="*50)
        print("MANUAL QA VERIFICATION")
        print("="*50)
        print("Compare these values with manual Excel calculations:")
        print(f"Avg Duration: {self.all_data['appointment_duration_min'].mean():.2f}")
        print(f"Min Duration: {self.all_data['appointment_duration_min'].min():.2f}")
        print(f"Max Duration: {self.all_data['appointment_duration_min'].max():.2f}")
        print(f"Tuesdays: {sum(self.all_data['day_of_week'] == 'Tuesday')}")
        print(f"9 AM appointments: {sum(self.all_data['hour_of_day'] == 9)}")
        print(f"2 PM appointments: {sum(self.all_data['hour_of_day'] == 14)}")
        print(f"Interpreter Count (script): {self.all_data['interpreter_needed'].sum()}")
        print("="*50)

    def calculate_normalized_busiest_days(self):
        # Figure out average clients per day, accounting for the fact that
        # some weekdays might appear more often than others in the month
        if self.all_data.empty:
            return pd.DataFrame()
        
        # Count how many times each weekday appears in our data
        weekday_counts = Counter(self.all_data['day_of_week'].dropna())
        
        # Count total clients by day of week
        clients_by_day = self.all_data.groupby('day_of_week').size()
        
        # Calculate average clients per weekday occurrence
        avg_clients = {}
        for day in clients_by_day.index:
            if weekday_counts.get(day, 0) > 0:
                avg_clients[day] = clients_by_day[day] / weekday_counts[day]
            else:
                avg_clients[day] = 0
        
        # Put it in a nice DataFrame
        avg_df = pd.DataFrame(list(avg_clients.items()), columns=["Day", "Avg_Clients_Per_Occurrence"])
        
        # Sort by proper day order (Monday first, Sunday last)
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        avg_df['Day'] = pd.Categorical(avg_df['Day'], categories=day_order, ordered=True)
        avg_df = avg_df.sort_values('Day').reset_index(drop=True)
        
        return avg_df

    def create_busiest_days_chart(self, workbook, worksheet, days_df, start_row=0):
        # Make a bar chart showing which days are busiest
        chart = workbook.add_chart({'type': 'column'})
        
        # Tell it where to find the data
        max_row = len(days_df) + start_row
        chart.add_series({
            'name': 'Appointment Count',
            'categories': [worksheet.name, start_row + 1, 0, max_row, 0],
            'values': [worksheet.name, start_row + 1, 1, max_row, 1],
            'fill': {'color': '#4472C4'},
        })
        
        chart.set_title({'name': 'Busiest Days of the Week'})
        chart.set_x_axis({'name': 'Day of Week'})
        chart.set_y_axis({'name': 'Number of Appointments'})
        chart.set_size({'width': 480, 'height': 288})
        
        # Put the chart below the data
        worksheet.insert_chart(max_row + 3, 0, chart)

    def create_avg_clients_chart(self, workbook, worksheet, avg_df, start_row=0):
        # Make a chart for the normalized average clients per day
        chart = workbook.add_chart({'type': 'column'})
        
        # Set up the data range
        max_row = len(avg_df) + start_row
        chart.add_series({
            'name': 'Avg Clients per Occurrence',
            'categories': [worksheet.name, start_row + 1, 0, max_row, 0],
            'values': [worksheet.name, start_row + 1, 1, max_row, 1],
            'fill': {'color': '#E67E22'},
        })
        
        chart.set_title({'name': 'Average Clients per Weekday Occurrence (Normalized)'})
        chart.set_x_axis({'name': 'Day of Week'})
        chart.set_y_axis({'name': 'Average Clients per Day Occurrence'})
        chart.set_size({'width': 480, 'height': 288})
        
        # Put the chart below the data
        worksheet.insert_chart(max_row + 3, 0, chart)

    def create_busiest_hours_chart(self, workbook, worksheet, hours_df, start_row=0):
        # Make a chart showing which hours are busiest
        chart = workbook.add_chart({'type': 'column'})
        
        # Set up the data range
        max_row = len(hours_df) + start_row
        chart.add_series({
            'name': 'Appointment Count',
            'categories': [worksheet.name, start_row + 1, 0, max_row, 0],
            'values': [worksheet.name, start_row + 1, 1, max_row, 1],
            'fill': {'color': '#70AD47'},
        })
        
        chart.set_title({'name': 'Busiest Appointment Hours'})
        chart.set_x_axis({'name': 'Hour of Day'})
        chart.set_y_axis({'name': 'Number of Appointments'})
        chart.set_size({'width': 480, 'height': 288})
        
        # Put the chart below the data
        worksheet.insert_chart(max_row + 3, 0, chart)

    def create_duration_histogram(self, workbook, worksheet, duration_data, start_row=0):
        # Create a histogram showing how appointment durations are distributed
        # Create bins every 20 minutes: 0-20, 20-40, etc.
        bins = np.arange(0, 201, 20)
        hist, bin_edges = np.histogram(duration_data, bins=bins)
        
        # Make nice labels for the bins
        bin_labels = [f"{int(bin_edges[i])}-{int(bin_edges[i+1])}" for i in range(len(hist))]
        
        # Write the histogram data to the worksheet
        histogram_start_row = start_row
        worksheet.write(histogram_start_row, 3, 'Duration Range (min)')
        worksheet.write(histogram_start_row, 4, 'Count')
        
        for i, (label, count) in enumerate(zip(bin_labels, hist)):
            worksheet.write(histogram_start_row + 1 + i, 3, label)
            worksheet.write(histogram_start_row + 1 + i, 4, count)
        
        # Create the chart
        chart = workbook.add_chart({'type': 'column'})
        
        max_hist_row = histogram_start_row + len(hist)
        chart.add_series({
            'name': 'Frequency',
            'categories': [worksheet.name, histogram_start_row + 1, 3, max_hist_row, 3],
            'values': [worksheet.name, histogram_start_row + 1, 4, max_hist_row, 4],
            'fill': {'color': '#FFC000'},
        })
        
        chart.set_title({'name': 'Appointment Duration Distribution'})
        chart.set_x_axis({'name': 'Duration Range (minutes)'})
        chart.set_y_axis({'name': 'Number of Appointments'})
        chart.set_size({'width': 480, 'height': 288})
        
        # Put the chart to the right of the data
        worksheet.insert_chart(histogram_start_row, 6, chart)

    def create_interpreter_chart(self, workbook, worksheet, interpreter_data):
        # Make a chart comparing appointment lengths with/without interpreters
        chart = workbook.add_chart({'type': 'column'})
        
        # Set up the data (just 2 bars)
        chart.add_series({
            'name': 'Average Duration (minutes)',
            'categories': [worksheet.name, 1, 0, 2, 0],
            'values': [worksheet.name, 1, 1, 2, 1],
            'fill': {'color': '#C55A5A'},
        })
        
        chart.set_title({'name': 'Average Appointment Duration: With vs Without Interpreter'})
        chart.set_x_axis({'name': 'Interpreter Status'})
        chart.set_y_axis({'name': 'Average Duration (minutes)'})
        chart.set_size({'width': 480, 'height': 288})
        
        # Put the chart below the data
        worksheet.insert_chart(5, 0, chart)

    def save_results(self, stats, busiest):
        # Save everything to an Excel file with multiple sheets and charts
        if self.all_data.empty:
            print("No data to save!")
            return
            
        print(f"Saving results with charts to {self.output_file}...")

        # Use xlsxwriter so we can add charts
        workbook = xlsxwriter.Workbook(self.output_file, {'nan_inf_to_errors': True})
        
        # Set up some nice formatting
        header_format = workbook.add_format({'bold': True, 'bg_color': '#D9E1F2'})
        
        # Sheet 1: All the raw processed data
        if not self.all_data.empty:
            worksheet1 = workbook.add_worksheet('All Processed Data')
            
            # Write column headers
            for col_num, column in enumerate(self.all_data.columns):
                worksheet1.write(0, col_num, column, header_format)
            
            # Write all the data
            for row_num, row_data in enumerate(self.all_data.values):
                for col_num, value in enumerate(row_data):
                    if pd.isna(value):
                        worksheet1.write(row_num + 1, col_num, '')
                    else:
                        worksheet1.write(row_num + 1, col_num, value)

        # Sheet 1a: Duration histogram (separate sheet to avoid layout issues)
        if not self.all_data.empty:
            duration_data = self.all_data['appointment_duration_min'].dropna()
            if not duration_data.empty:
                worksheet_hist = workbook.add_worksheet('Duration Histogram')
                self.create_duration_histogram(workbook, worksheet_hist, duration_data, start_row=0)

        # Sheet 2: Summary statistics
        if stats:
            worksheet2 = workbook.add_worksheet('Summary Statistics')
            worksheet2.write(0, 0, 'Statistic', header_format)
            worksheet2.write(0, 1, 'Value', header_format)
            
            for row_num, (key, val) in enumerate(stats.items()):
                worksheet2.write(row_num + 1, 0, key)
                if isinstance(val, float):
                    worksheet2.write(row_num + 1, 1, f"{val:.2f}")
                else:
                    worksheet2.write(row_num + 1, 1, val)

        # Sheet 3: Busiest days with a chart
        if 'busiest_days' in busiest and busiest['busiest_days']:
            worksheet3 = workbook.add_worksheet('Busiest Days')
            
            # Convert to DataFrame and sort by proper day order
            days_df = pd.DataFrame(list(busiest['busiest_days'].items()), columns=['Day', 'Count'])
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            days_df['Day'] = pd.Categorical(days_df['Day'], categories=day_order, ordered=True)
            days_df = days_df.sort_values('Day').reset_index(drop=True)
            
            # Write the data
            worksheet3.write(0, 0, 'Day', header_format)
            worksheet3.write(0, 1, 'Count', header_format)
            
            for row_num, (day, count) in enumerate(zip(days_df['Day'], days_df['Count'])):
                worksheet3.write(row_num + 1, 0, str(day))
                worksheet3.write(row_num + 1, 1, count)
            
            # Add the chart
            self.create_busiest_days_chart(workbook, worksheet3, days_df)

        # Sheet 4: Busiest hours with a chart
        if 'busiest_hours' in busiest and busiest['busiest_hours']:
            worksheet4 = workbook.add_worksheet('Busiest Hours')
            
            # Convert to DataFrame and sort by hour
            hours_df = pd.DataFrame(list(busiest['busiest_hours'].items()), columns=['Hour', 'Count'])
            hours_df = hours_df.sort_values('Hour').reset_index(drop=True)
            
            # Write the data
            worksheet4.write(0, 0, 'Hour', header_format)
            worksheet4.write(0, 1, 'Count', header_format)
            
            for row_num, (hour, count) in enumerate(zip(hours_df['Hour'], hours_df['Count'])):
                worksheet4.write(row_num + 1, 0, int(hour))
                worksheet4.write(row_num + 1, 1, count)
            
            # Add the chart
            self.create_busiest_hours_chart(workbook, worksheet4, hours_df)

        # Sheet 5: Interpreter comparison chart
        worksheet5 = workbook.add_worksheet('Interpreter Chart')
        
        # Calculate the interpreter impact data
        interpreter_data = []
        if not self.all_data.empty:
            with_interp = self.all_data[self.all_data['interpreter_needed'] == True]['appointment_duration_min'].dropna()
            without_interp = self.all_data[self.all_data['interpreter_needed'] == False]['appointment_duration_min'].dropna()
            
            if not with_interp.empty:
                interpreter_data.append(['With Interpreter', with_interp.mean()])
            if not without_interp.empty:
                interpreter_data.append(['Without Interpreter', without_interp.mean()])
        
        # Write the data
        worksheet5.write(0, 0, 'Interpreter Status', header_format)
        worksheet5.write(0, 1, 'Average Duration (minutes)', header_format)
        
        for row_num, (status, duration) in enumerate(interpreter_data):
            worksheet5.write(row_num + 1, 0, status)
            worksheet5.write(row_num + 1, 1, f"{duration:.2f}")
        
        # Add the chart
        if interpreter_data:
            self.create_interpreter_chart(workbook, worksheet5, interpreter_data)

        # Sheet 6: Normalized busiest days (accounts for uneven weekday counts)
        avg_clients_df = self.calculate_normalized_busiest_days()
        if not avg_clients_df.empty:
            worksheet6 = workbook.add_worksheet('Avg Clients per Weekday')
            
            # Write the data
            worksheet6.write(0, 0, 'Day', header_format)
            worksheet6.write(0, 1, 'Avg Clients per Occurrence', header_format)
            
            for row_num, (day, avg_count) in enumerate(zip(avg_clients_df['Day'], avg_clients_df['Avg_Clients_Per_Occurrence'])):
                worksheet6.write(row_num + 1, 0, str(day))
                worksheet6.write(row_num + 1, 1, f"{avg_count:.2f}")
            
            # Add the chart
            self.create_avg_clients_chart(workbook, worksheet6, avg_clients_df)

        workbook.close()
        print("Results with charts saved successfully!")

    def run_analysis(self):
        # This is the main function that runs everything
        print("\n" + "="*60)
        print("WIC CLINIC MONTHLY ANALYSIS")
        print("="*60)
        
        self.load_and_process_all_sheets()
        
        if self.all_data.empty:
            print("No valid data found. Analysis cannot proceed.")
            return
            
        stats, busiest = self.calculate_statistics()

        # Show some key numbers for manual checking
        self.perform_qa_verification()

        print("\n" + "="*40)
        print("MONTHLY SUMMARY STATISTICS")
        print("="*40)
        for key, val in stats.items():
            if isinstance(val, float):
                print(f"  {key}: {val:.2f}")
            else:
                print(f"  {key}: {val}")

        self.save_results(stats, busiest)
        print(f"\nMonthly analysis completed successfully!")
        print(f"Results saved to: {self.output_file}")

def main():
    # Set up the analyzer with input and output files
    analyzer = WICClinicAnalyzerMonthly(
        input_file='2522 master clinic.xlsx',
        output_file='2522_monthly_analysis_with_charts.xlsx'
    )
    analyzer.run_analysis()

if __name__ == "__main__":
    main()