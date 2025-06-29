import pandas as pd
import numpy as np

class WICClinicAnalyzerMonthly:
    """Analyze WIC clinic data across multiple daily sheets in one Excel file."""

    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file
        self.all_data = pd.DataFrame()
        self.sheet_names = []

    def load_and_process_all_sheets(self):
        """Load and process all daily sheets in the workbook, skipping 'Totals'."""
        print(f"Loading workbook: {self.input_file}...")
        excel_file = pd.ExcelFile(self.input_file)
        self.sheet_names = [s for s in excel_file.sheet_names if s.lower() != 'totals']

        print(f"Found sheets: {self.sheet_names}")

        processed_frames = []

        for sheet in self.sheet_names:
            print(f"\nProcessing sheet: {sheet}...")
            df = pd.read_excel(self.input_file, sheet_name=sheet)
            df['sheet_date'] = sheet  # track which sheet the data came from

            # Map datetime columns
            datetime_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]

            time_mapping = {}
            if len(datetime_cols) >= 1: time_mapping['FRONT DESK'] = datetime_cols[0]
            if len(datetime_cols) >= 2: time_mapping['INTAKE UP'] = datetime_cols[1]
            if len(datetime_cols) >= 3: time_mapping['PEER UP'] = datetime_cols[2]
            if len(datetime_cols) >= 4: time_mapping['HP UP'] = datetime_cols[3]
            if len(datetime_cols) >= 5: time_mapping['Finish Time - @ end of HP'] = datetime_cols[4]

            for logical_name, actual_col in time_mapping.items():
                df[logical_name] = df[actual_col]

            # Find language and comments columns
            for col in df.columns:
                col_sample = str(df[col].iloc[1]).lower() if len(df) > 1 else ""
                if 'language' in col_sample:
                    df['language'] = df[col]
                elif 'comment' in col_sample:
                    df['Comments'] = df[col]

            # Calculate appointment durations
            if 'FRONT DESK' in df.columns and 'Finish Time - @ end of HP' in df.columns:
                start_times = pd.to_datetime(df['FRONT DESK'], errors='coerce')
                end_times = pd.to_datetime(df['Finish Time - @ end of HP'], errors='coerce')
                df['appointment_duration_min'] = (end_times - start_times).dt.total_seconds() / 60
            else:
                df['appointment_duration_min'] = np.nan

            # Calculate transition times
            time_cols = ['FRONT DESK', 'INTAKE UP', 'PEER UP', 'HP UP']
            for i in range(len(time_cols) - 1):
                if time_cols[i] in df.columns and time_cols[i + 1] in df.columns:
                    start_col = time_cols[i]
                    end_col = time_cols[i + 1]
                    transition_name = f"transition_{start_col.lower().replace(' ', '_')}_to_{end_col.lower().replace(' ', '_')}"
                    start_times = pd.to_datetime(df[start_col], errors='coerce')
                    end_times = pd.to_datetime(df[end_col], errors='coerce')
                    df[transition_name] = (end_times - start_times).dt.total_seconds() / 60

            # Identify interpreter needs
            df['interpreter_needed'] = False
            for col in ['language', 'Comments', 'Comments ']:
                if col in df.columns:
                    lang_col = df[col].astype(str).str.lower()
                    non_english = ~lang_col.isin(['english', 'eng', 'en', 'nan', ''])
                    df['interpreter_needed'] = df['interpreter_needed'] | non_english

            # Extract time features
            if 'FRONT DESK' in df.columns:
                appointment_times = pd.to_datetime(df['FRONT DESK'], errors='coerce')
                df['day_of_week'] = appointment_times.dt.day_name()
                df['hour_of_day'] = appointment_times.dt.hour
            else:
                df['day_of_week'] = np.nan
                df['hour_of_day'] = np.nan

            processed_frames.append(df)

        self.all_data = pd.concat(processed_frames, ignore_index=True)
        print("\nAll sheets processed and combined successfully!")

    def calculate_statistics(self):
        """Calculate overall statistics for the entire month."""
        stats = {}
        busiest = {}

        # Durations
        valid_durations = self.all_data['appointment_duration_min'].dropna()
        stats['Average Appointment Duration (minutes)'] = valid_durations.mean()
        stats['Median Appointment Duration (minutes)'] = valid_durations.median()
        stats['Max Appointment Duration (minutes)'] = valid_durations.max()
        stats['Min Appointment Duration (minutes)'] = valid_durations.min()

        # Interpreter impact
        interpreter = self.all_data[self.all_data['interpreter_needed']]
        if not interpreter.empty:
            stats['Duration WITH Interpreter (minutes)'] = interpreter['appointment_duration_min'].mean()
        no_interpreter = self.all_data[~self.all_data['interpreter_needed']]
        if not no_interpreter.empty:
            stats['Duration WITHOUT Interpreter (minutes)'] = no_interpreter['appointment_duration_min'].mean()

        # Busiest days/hours
        day_counts = self.all_data['day_of_week'].value_counts()
        busiest['busiest_days'] = day_counts.to_dict()
        stats['Busiest Day of Week'] = day_counts.idxmax() if not day_counts.empty else 'N/A'

        hour_counts = self.all_data['hour_of_day'].value_counts().sort_index()
        busiest['busiest_hours'] = hour_counts.to_dict()
        stats['Busiest Hour of Day'] = hour_counts.idxmax() if not hour_counts.empty else 'N/A'

        return stats, busiest

    def save_results(self, stats, busiest):
        """Save full processed data and statistics to Excel."""
        print(f"Saving results to {self.output_file}...")

        with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
            # Save all processed data
            self.all_data.to_excel(writer, sheet_name='All Processed Data', index=False)

            # Save summary statistics
            stats_df = pd.DataFrame(list(stats.items()), columns=['Statistic', 'Value'])
            stats_df.to_excel(writer, sheet_name='Summary Statistics', index=False)

            # Save busiest times
            if 'busiest_days' in busiest:
                days_df = pd.DataFrame(list(busiest['busiest_days'].items()), columns=['Day', 'Count'])
                days_df.to_excel(writer, sheet_name='Busiest Days', index=False)
            if 'busiest_hours' in busiest:
                hours_df = pd.DataFrame(list(busiest['busiest_hours'].items()), columns=['Hour', 'Count'])
                hours_df.to_excel(writer, sheet_name='Busiest Hours', index=False)

        print("Results saved successfully!")

    def run_analysis(self):
        """Run the entire monthly analysis pipeline."""
        print("\nWIC Clinic Monthly Analysis Starting...")
        self.load_and_process_all_sheets()
        stats, busiest = self.calculate_statistics()

        print("\nMonthly Summary Statistics:")
        for key, val in stats.items():
            if isinstance(val, float):
                print(f"  {key}: {val:.2f}")
            else:
                print(f"  {key}: {val}")

        self.save_results(stats, busiest)
        print("\nMonthly analysis completed successfully!")

def main():
    analyzer = WICClinicAnalyzerMonthly(
        input_file='2522 master clinic.xlsx',
        output_file='2522_monthly_analysis_results.xlsx'
    )
    analyzer.run_analysis()

if __name__ == "__main__":
    main()
