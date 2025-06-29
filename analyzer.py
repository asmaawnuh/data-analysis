import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class WICClinicAnalyzerMonthly:
    """Analyze WIC clinic data across multiple daily sheets in one Excel file."""

    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file
        self.all_data = pd.DataFrame()
        self.sheet_names = []

    def load_and_process_all_sheets(self):
        """Load and process all daily sheets, handling unnamed columns and cross-date issues."""
        print(f"Loading workbook: {self.input_file}...")
        excel_file = pd.ExcelFile(self.input_file)
        self.sheet_names = [s for s in excel_file.sheet_names if s.lower() != 'totals']

        print(f"Found sheets: {self.sheet_names}")

        processed_frames = []

        for sheet in self.sheet_names:
            print(f"\nProcessing sheet: {sheet}...")
            df = pd.read_excel(self.input_file, sheet_name=sheet)
            
            # Skip if sheet is too small
            if df.shape[0] < 3:
                print(f"  Skipping {sheet} - too few rows ({df.shape[0]})")
                continue
                
            df['sheet_date'] = sheet
            
            # Get datetime columns in order
            datetime_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
            print(f"  Found {len(datetime_cols)} datetime columns: {datetime_cols}")
            
            if len(datetime_cols) < 2:
                print(f"  Skipping {sheet} - not enough datetime columns")
                continue

            # Map datetime columns to logical names based on position
            # Based on the sample data, typical order seems to be:
            # Unnamed: 10 = FRONT DESK (start)
            # Unnamed: 12 = INTAKE UP 
            # Unnamed: 14 = PEER UP
            # Unnamed: 20 = HP UP
            # Unnamed: 23 = Finish Time (end)
            
            column_mapping = {}
            if len(datetime_cols) >= 1: column_mapping['FRONT DESK'] = datetime_cols[0]
            if len(datetime_cols) >= 2: column_mapping['INTAKE UP'] = datetime_cols[1]
            if len(datetime_cols) >= 3: column_mapping['PEER UP'] = datetime_cols[2]
            if len(datetime_cols) >= 4: column_mapping['HP UP'] = datetime_cols[3]
            if len(datetime_cols) >= 5: column_mapping['Finish Time - @ end of HP'] = datetime_cols[4]
            
            # If we have fewer than 5 columns, assume first is start and last is end
            if len(datetime_cols) >= 2:
                column_mapping['FRONT DESK'] = datetime_cols[0]
                column_mapping['Finish Time - @ end of HP'] = datetime_cols[-1]

            # Copy datetime columns with logical names
            for logical_name, actual_col in column_mapping.items():
                df[logical_name] = df[actual_col]

            # CRITICAL: Filter out cross-date contamination
            # Parse the sheet date to get expected date range
            expected_date = self._parse_sheet_date(sheet)
            if expected_date:
                print(f"  Expected date for sheet {sheet}: {expected_date}")
                df = self._filter_cross_date_contamination(df, expected_date, datetime_cols)

            # Find language and comments columns by checking content
            language_col = None
            comments_col = None
            
            for col in df.columns:
                if df[col].dtype == 'object':  # Text columns
                    sample_values = df[col].dropna().astype(str).str.lower()
                    if len(sample_values) > 0:
                        # Check if this looks like a language column
                        if any(lang in ' '.join(sample_values) for lang in ['english', 'spanish', 'arabic', 'french']):
                            language_col = col
                            df['language'] = df[col]
                        # Check if this looks like a comments column
                        elif any(word in ' '.join(sample_values) for word in ['comment', 'note', 'interpreter']):
                            comments_col = col
                            df['Comments'] = df[col]

            # Calculate appointment durations ONLY for same-day appointments
            if 'FRONT DESK' in df.columns and 'Finish Time - @ end of HP' in df.columns:
                start_times = pd.to_datetime(df['FRONT DESK'], errors='coerce')
                end_times = pd.to_datetime(df['Finish Time - @ end of HP'], errors='coerce')
                
                # Calculate raw durations
                raw_durations = (end_times - start_times).dt.total_seconds() / 60
                
                # Apply realistic filters
                # 1. Must be positive
                # 2. Must be less than 8 hours (480 minutes) 
                # 3. Must be more than 5 minutes (sanity check)
                valid_mask = (
                    (raw_durations > 5) & 
                    (raw_durations < 480) & 
                    (raw_durations.notna())
                )
                
                df['appointment_duration_min'] = np.where(valid_mask, raw_durations, np.nan)
                
                # Report filtering results
                total_records = len(df)
                valid_records = valid_mask.sum()
                print(f"  Duration filtering: {valid_records}/{total_records} records kept")
                if valid_records < total_records:
                    invalid_durations = raw_durations[~valid_mask].dropna()
                    if len(invalid_durations) > 0:
                        print(f"  Filtered out durations: min={invalid_durations.min():.1f}, max={invalid_durations.max():.1f}")
            else:
                df['appointment_duration_min'] = np.nan

            # Calculate transition times between workflow steps
            transition_cols = ['FRONT DESK', 'INTAKE UP', 'PEER UP', 'HP UP']
            for i in range(len(transition_cols) - 1):
                if transition_cols[i] in df.columns and transition_cols[i + 1] in df.columns:
                    start_col = transition_cols[i]
                    end_col = transition_cols[i + 1]
                    transition_name = f"transition_{start_col.lower().replace(' ', '_')}_to_{end_col.lower().replace(' ', '_')}"
                    
                    start_times = pd.to_datetime(df[start_col], errors='coerce')
                    end_times = pd.to_datetime(df[end_col], errors='coerce')
                    transition_duration = (end_times - start_times).dt.total_seconds() / 60
                    
                    # Filter transition times (should be 0-120 minutes)
                    valid_transitions = (transition_duration >= 0) & (transition_duration <= 120)
                    df[transition_name] = np.where(valid_transitions, transition_duration, np.nan)

            # Identify interpreter needs
            df['interpreter_needed'] = False
            if 'language' in df.columns:
                lang_col = df['language'].astype(str).str.lower()
                non_english = ~lang_col.isin(['english', 'eng', 'en', 'nan', '', 'none'])
                df['interpreter_needed'] = non_english
            elif 'Comments' in df.columns:
                comments_col = df['Comments'].astype(str).str.lower()
                interpreter_keywords = ['interpreter', 'spanish', 'arabic', 'french', 'translate']
                has_interpreter_ref = comments_col.str.contains('|'.join(interpreter_keywords), na=False)
                df['interpreter_needed'] = has_interpreter_ref

            # Extract time features
            if 'FRONT DESK' in df.columns:
                appointment_times = pd.to_datetime(df['FRONT DESK'], errors='coerce')
                df['day_of_week'] = appointment_times.dt.day_name()
                df['hour_of_day'] = appointment_times.dt.hour
            else:
                df['day_of_week'] = np.nan
                df['hour_of_day'] = np.nan

            # Only keep rows with valid appointment times
            valid_rows = df['appointment_duration_min'].notna()
            df_filtered = df[valid_rows].copy()
            
            print(f"  Final records for {sheet}: {len(df_filtered)}")
            if len(df_filtered) > 0:
                processed_frames.append(df_filtered)

        if processed_frames:
            self.all_data = pd.concat(processed_frames, ignore_index=True)
            print(f"\nAll sheets processed! Total valid records: {len(self.all_data)}")
        else:
            print("\nWARNING: No valid data found in any sheets!")
            self.all_data = pd.DataFrame()

    def _parse_sheet_date(self, sheet_name):
        """Parse the expected date from sheet name."""
        try:
            # Handle sheet names like '2522 0101' or '0102'
            if sheet_name.startswith('2522'):
                date_part = sheet_name.split()[-1]  # Get last part
            else:
                date_part = sheet_name
            
            # Assume 2025 for now (adjust as needed)
            month = int(date_part[:2])
            day = int(date_part[2:])
            year = 2025  # Adjust based on your data
            
            return datetime(year, month, day).date()
        except:
            return None

    def _filter_cross_date_contamination(self, df, expected_date, datetime_cols):
        """Filter out appointments that span across different dates."""
        filtered_df = df.copy()
        
        for col in datetime_cols:
            if col in filtered_df.columns:
                times = pd.to_datetime(filtered_df[col], errors='coerce')
                # Keep only times that are within 1 day of expected date
                valid_dates = times.dt.date
                date_mask = (
                    (valid_dates >= expected_date - timedelta(days=1)) & 
                    (valid_dates <= expected_date + timedelta(days=1))
                )
                filtered_df.loc[~date_mask, col] = pd.NaT
        
        return filtered_df

    def calculate_statistics(self):
        """Calculate overall statistics for the entire month."""
        if self.all_data.empty:
            return {}, {}
            
        stats = {}
        busiest = {}

        # Durations
        valid_durations = self.all_data['appointment_duration_min'].dropna()
        if not valid_durations.empty:
            stats['Total Valid Appointments'] = len(valid_durations)
            stats['Average Appointment Duration (minutes)'] = valid_durations.mean()
            stats['Median Appointment Duration (minutes)'] = valid_durations.median()
            stats['Max Appointment Duration (minutes)'] = valid_durations.max()
            stats['Min Appointment Duration (minutes)'] = valid_durations.min()
            stats['Std Dev Appointment Duration (minutes)'] = valid_durations.std()

        # Interpreter impact
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

        # Busiest days/hours
        day_counts = self.all_data['day_of_week'].value_counts()
        if not day_counts.empty:
            busiest['busiest_days'] = day_counts.to_dict()
            stats['Busiest Day of Week'] = day_counts.idxmax()

        hour_counts = self.all_data['hour_of_day'].value_counts().sort_index()
        if not hour_counts.empty:
            busiest['busiest_hours'] = hour_counts.to_dict()
            stats['Busiest Hour of Day'] = hour_counts.idxmax()

        return stats, busiest

    def save_results(self, stats, busiest):
        """Save full processed data and statistics to Excel."""
        if self.all_data.empty:
            print("No data to save!")
            return
            
        print(f"Saving results to {self.output_file}...")

        with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
            # Save all processed data
            self.all_data.to_excel(writer, sheet_name='All Processed Data', index=False)

            # Save summary statistics
            if stats:
                stats_df = pd.DataFrame(list(stats.items()), columns=['Statistic', 'Value'])
                stats_df.to_excel(writer, sheet_name='Summary Statistics', index=False)

            # Save busiest times
            if 'busiest_days' in busiest and busiest['busiest_days']:
                days_df = pd.DataFrame(list(busiest['busiest_days'].items()), columns=['Day', 'Count'])
                days_df.to_excel(writer, sheet_name='Busiest Days', index=False)
            if 'busiest_hours' in busiest and busiest['busiest_hours']:
                hours_df = pd.DataFrame(list(busiest['busiest_hours'].items()), columns=['Hour', 'Count'])
                hours_df.to_excel(writer, sheet_name='Busiest Hours', index=False)

        print("Results saved successfully!")

    def run_analysis(self):
        """Run the entire monthly analysis pipeline."""
        print("\n" + "="*60)
        print("WIC CLINIC MONTHLY ANALYSIS")
        print("="*60)
        
        self.load_and_process_all_sheets()
        
        if self.all_data.empty:
            print("No valid data found. Analysis cannot proceed.")
            return
            
        stats, busiest = self.calculate_statistics()

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
    analyzer = WICClinicAnalyzerMonthly(
        input_file='2522 master clinic.xlsx',
        output_file='2522_monthly_analysis_results.xlsx'
    )
    analyzer.run_analysis()

if __name__ == "__main__":
    main()