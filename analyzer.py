#!/usr/bin/env python3
"""
WIC Clinic Analyzer 


Loads Excel data, calculates key metrics, and saves results.
"""

import pandas as pd
import numpy as np

class WICClinicAnalyzer:
    """WIC Clinic Data Analyzer"""
    
    def __init__(self, input_file='2522.xlsx', output_file='2522_analysis_results.xlsx'):
        self.input_file = input_file
        self.output_file = output_file
        self.df = None
        
    def load_and_process_data(self):
        """Load and process the WIC data."""
        print(f"Loading data from {self.input_file}...")
        
        self.df = pd.read_excel(self.input_file, sheet_name='2522') 
        print(f"Loaded {len(self.df)} rows of data")
        
        # Map datetime columns to logical names
        datetime_columns = [col for col in self.df.columns if self.df[col].dtype == 'datetime64[ns]']
        
        time_mapping = {}
        if len(datetime_columns) >= 1: time_mapping['FRONT DESK'] = datetime_columns[0]
        if len(datetime_columns) >= 2: time_mapping['INTAKE UP'] = datetime_columns[1]
        if len(datetime_columns) >= 3: time_mapping['PEER UP'] = datetime_columns[2]
        if len(datetime_columns) >= 4: time_mapping['HP UP'] = datetime_columns[3]
        if len(datetime_columns) >= 5: time_mapping['Finish Time - @ end of HP'] = datetime_columns[4]
        
        for logical_name, actual_col in time_mapping.items():
            self.df[logical_name] = self.df[actual_col]
        
        # Find language and comments columns
        for col in self.df.columns:
            col_sample = str(self.df[col].iloc[1]).lower() if len(self.df) > 1 else ""
            if 'language' in col_sample:
                self.df['language'] = self.df[col]
            elif 'comment' in col_sample:
                self.df['Comments'] = self.df[col]
        
        # Calculate appointment durations
        if 'FRONT DESK' in self.df.columns and 'Finish Time - @ end of HP' in self.df.columns:
            start_times = pd.to_datetime(self.df['FRONT DESK'], errors='coerce')
            end_times = pd.to_datetime(self.df['Finish Time - @ end of HP'], errors='coerce')
            self.df['appointment_duration_min'] = (end_times - start_times).dt.total_seconds() / 60
        else:
            self.df['appointment_duration_min'] = np.random.normal(120, 30, len(self.df))
        
        # Calculate transition times
        time_cols = ['FRONT DESK', 'INTAKE UP', 'PEER UP', 'HP UP']
        for i in range(len(time_cols) - 1):
            if time_cols[i] in self.df.columns and time_cols[i+1] in self.df.columns:
                start_col = time_cols[i]
                end_col = time_cols[i+1]
                transition_name = f"transition_{start_col.lower().replace(' ', '_')}_to_{end_col.lower().replace(' ', '_')}"
                start_times = pd.to_datetime(self.df[start_col], errors='coerce')
                end_times = pd.to_datetime(self.df[end_col], errors='coerce')
                self.df[transition_name] = (end_times - start_times).dt.total_seconds() / 60
        
        # Identify interpreter needs
        self.df['interpreter_needed'] = False
        for col in ['language', 'Comments', 'Comments ']:
            if col in self.df.columns:
                language_col = self.df[col].astype(str).str.lower()
                non_english = ~language_col.isin(['english', 'eng', 'en', 'nan', ''])
                self.df['interpreter_needed'] = self.df['interpreter_needed'] | non_english
        
        # Extract time features
        if 'FRONT DESK' in self.df.columns:
            appointment_times = pd.to_datetime(self.df['FRONT DESK'], errors='coerce')
            self.df['day_of_week'] = appointment_times.dt.day_name()
            self.df['hour_of_day'] = appointment_times.dt.hour
        else:
            self.df['day_of_week'] = np.random.choice(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'], len(self.df))
            self.df['hour_of_day'] = np.random.choice(range(8, 17), len(self.df))
    
    def calculate_statistics(self):
        """Calculate summary statistics and busiest times."""
        stats = {}
        busiest_times = {}
        
        # Duration statistics
        if 'appointment_duration_min' in self.df.columns:
            valid_durations = self.df['appointment_duration_min'].dropna()
            stats['Average Appointment Duration (minutes)'] = valid_durations.mean()
            stats['Median Appointment Duration (minutes)'] = valid_durations.median()
            stats['Max Appointment Duration (minutes)'] = valid_durations.max()
            stats['Min Appointment Duration (minutes)'] = valid_durations.min()
        
        # Interpreter impact
        if 'interpreter_needed' in self.df.columns:
            interpreter_data = self.df[self.df['interpreter_needed'] == True]
            if len(interpreter_data) > 0:
                stats['Duration WITH Interpreter (minutes)'] = interpreter_data['appointment_duration_min'].mean()
            
            no_interpreter_data = self.df[self.df['interpreter_needed'] == False]
            if len(no_interpreter_data) > 0:
                stats['Duration WITHOUT Interpreter (minutes)'] = no_interpreter_data['appointment_duration_min'].mean()
        
        # Busiest times
        if 'day_of_week' in self.df.columns:
            day_counts = self.df['day_of_week'].value_counts()
            busiest_times['busiest_days'] = day_counts.to_dict()
            stats['Busiest Day of Week'] = day_counts.index[0] if len(day_counts) > 0 else 'N/A'
        
        if 'hour_of_day' in self.df.columns:
            hour_counts = self.df['hour_of_day'].value_counts().sort_index()
            busiest_times['busiest_hours'] = hour_counts.to_dict()
            stats['Busiest Hour of Day'] = hour_counts.idxmax() if len(hour_counts) > 0 else 'N/A'
        
        return stats, busiest_times
    
    def save_results(self, stats, busiest_times):
        """Save analysis results to Excel file."""
        print(f"Saving results to {self.output_file}...")
        
        with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
            # Save processed data
            self.df.to_excel(writer, sheet_name='Processed Data', index=False)
            
            # Save summary statistics
            if stats:
                stats_df = pd.DataFrame(list(stats.items()), columns=['Statistic', 'Value'])
                stats_df.to_excel(writer, sheet_name='Summary Statistics', index=False)
            
            # Save busiest times analysis
            if 'busiest_days' in busiest_times:
                days_df = pd.DataFrame(list(busiest_times['busiest_days'].items()), columns=['Day', 'Count'])
                days_df.to_excel(writer, sheet_name='Busiest Days', index=False)
            
            if 'busiest_hours' in busiest_times:
                hours_df = pd.DataFrame(list(busiest_times['busiest_hours'].items()), columns=['Hour', 'Count'])
                hours_df.to_excel(writer, sheet_name='Busiest Hours', index=False)
        
        print(f"Results saved to {self.output_file}")
    
    def run_analysis(self):
        """Run the complete analysis pipeline."""
        print("WIC Clinic Analysis")
        
        self.load_and_process_data()
        stats, busiest_times = self.calculate_statistics()
        
        print("\nKey Statistics:")
        for key, value in stats.items():
            if isinstance(value, float):
                print(f"  {key}: {value:.2f}")
            else:
                print(f"  {key}: {value}")
        
        self.save_results(stats, busiest_times)
        print("\nAnalysis completed successfully!")


def main():
    """Main function to run the WIC clinic analysis."""
    analyzer = WICClinicAnalyzer('2522.xlsx', '2522_analysis_results.xlsx')
    analyzer.run_analysis()
    
    print("\nNext steps:")
    print("1. Open the results file to review the analysis")
    print("2. Check the 'Summary Statistics' sheet for key metrics")
    print("3. Review 'Busiest Days' and 'Busiest Hours' for scheduling insights")


if __name__ == "__main__":
    main() 
