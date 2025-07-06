import pandas as pd
import numpy as np
from datetime import datetime, timedelta
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

    def is_interpreter_needed_refined(self, language, comments):
        """
        Refined interpreter detection that reduces false positives.
        Only returns True if there's clear evidence of interpreter need.
        """
        # List of actual languages that indicate interpreter need
        actual_languages = [
            "spanish", "somali", "arabic", "vietnamese", "hmong", "amharic",
            "oromo", "karen", "french", "chinese", "mandarin", "cantonese",
            "tagalog", "portuguese", "russian", "german", "italian", "farsi",
            "korean", "japanese", "turkish", "polish", "thai", "laotian",
            "cambodian", "burmese", "tigrinya", "swahili", "urdu", "hindi",
            "punjabi", "bengali", "gujarati", "telugu", "tamil", "malayalam"
        ]
        
        # List of interpreter-related keywords in comments
        interpreter_keywords = [
            "interpreter", "translator", "translation", "language line",
            "somali interpreter", "spanish interpreter", "arabic interpreter",
            "vietnamese interpreter", "hmong interpreter", "amharic interpreter",
            "oromo interpreter", "karen interpreter", "french interpreter",
            "used language line", "needed translator", "non-english speaker",
            "language barrier", "translation needed", "interpreter needed",
            "language assistance", "bilingual staff", "translation services"
        ]
        
        # Keywords that are definitely NOT languages (common false positives)
        # These are appointment notes that get mistakenly flagged
        appointment_note_keywords = [
            "ht/wt", "height", "weight", "bf", "breastfeeding", "formula",
            "needs", "wants", "switch", "scheduled", "appointment", "rx",
            "prescription", "new", "check", "discussion", "change", "food",
            "package", "benefits", "referral", "visit", "education", "nutrition",
            "lactose", "free", "both", "kids", "need", "wic", "clinic"
        ]
        
        # Check language field - must be a valid language, not appointment notes
        lang_check = False
        if isinstance(language, str) and language.strip():
            lang_lower = language.strip().lower()
            
            # Skip if it's clearly an appointment note
            if any(note_kw in lang_lower for note_kw in appointment_note_keywords):
                lang_check = False
            # Skip if it's English variants
            elif lang_lower in ["english", "eng", "en", "none", "n/a", "na", "nan"]:
                lang_check = False
            # Check if it's a valid language
            elif any(lang in lang_lower for lang in actual_languages):
                lang_check = True
            # For short entries, be more strict - must be exact match to known language
            elif len(lang_lower.split()) <= 2:
                lang_check = lang_lower in actual_languages
        
        # Check comments for interpreter-related keywords
        comment_check = False
        if isinstance(comments, str) and comments.strip():
            comment_lower = comments.strip().lower()
            
            # Only flag if we have clear interpreter keywords
            has_interpreter_keyword = any(kw in comment_lower for kw in interpreter_keywords)
            
            if has_interpreter_keyword:
                comment_check = True
        
        return lang_check or comment_check

    def is_interpreter_needed_simple(self, language, comments):
        """
        Flexible interpreter detection that finds languages embedded in longer text.
        
        Criteria:
        - Detects languages even when part of longer strings (e.g. "spanish ht/wt")
        - Checks both language and comments columns
        - Excludes rows that contain ONLY medical shorthand or staff names
        """
        # Define the specific languages to detect
        VALID_LANGUAGES = {"spanish", "arabic", "somali", "dari", "french", "swahili", "haitian creole", "hmong", "russian", "nepali"}
        
        # Define false positive patterns to exclude (when they appear ALONE)
        FALSE_POSITIVE_PATTERNS = {
            # Medical shorthand
            "ht/wt", "no anthro", "preg ef", "hc", "rx", "poi", "poa", "pob", "pop",
            # Common WIC abbreviations
            "bf", "ef", "rc", "nc", "ha", "fk", "load", "pkg", "fd",
            # Generic words
            "comment", "walk in", "brand new", "see notes", "see comments",
            # Staff-related
            "katie", "mya", "ang", "tiana", "salim"
        }
        
        def contains_language_but_not_only_false_positive(text):
            """Check if text contains a language but isn't just medical shorthand"""
            if not isinstance(text, str) or not text.strip():
                return False
            
            # Clean the text
            text_clean = text.strip().lower()
            
            # Skip clearly empty entries
            if text_clean in ['', 'nan', 'none', 'n/a', 'na', 'english', 'eng', 'en']:
                return False
            
            # Check if any valid language is present
            language_found = any(language in text_clean for language in VALID_LANGUAGES)
            
            if not language_found:
                return False
            
            # If language is found, check if it's NOT just a false positive
            # Remove all language words and see what's left
            remaining_text = text_clean
            for language in VALID_LANGUAGES:
                remaining_text = remaining_text.replace(language, "")
            
            # Clean up the remaining text
            remaining_text = remaining_text.strip(" -,.:;")
            
            # If there's meaningful text left beyond just false positives, it's valid
            if remaining_text:
                # Check if remaining text is ONLY false positive patterns
                remaining_words = remaining_text.split()
                if remaining_words:
                    # If all remaining words are false positives, it's still valid
                    # because we found a language mixed with medical notes
                    return True
            
            # If only the language word remains, it's definitely valid
            return True
        
        # Check language column
        language_needs_interpreter = contains_language_but_not_only_false_positive(language)
        
        # Check comments column  
        comments_needs_interpreter = contains_language_but_not_only_false_positive(comments)
        
        return language_needs_interpreter or comments_needs_interpreter

    def validate_column_alignment(self, processed_frames):
        """
        Validate that all processed dataframes have the same column structure.
        Raises an error if column misalignment is detected.
        """
        if not processed_frames:
            return True
            
        expected_columns = [
            "date", "start_time", "end_time", "appointment_duration_min",
            "interpreter_needed", "day_of_week", "hour_of_day", "language", "Comments"
        ]
        
        print(f"\nValidating column alignment across {len(processed_frames)} sheets...")
        
        alignment_errors = []
        
        for i, df in enumerate(processed_frames):
            # Get actual sheet name from the dataframe if available
            sheet_name = df['_source_sheet'].iloc[0] if '_source_sheet' in df.columns else f"Sheet_{i+1}"
            
            # Check if columns match expected structure (excluding our temporary tracking column)
            actual_columns = [col for col in df.columns if col != '_source_sheet']
            if actual_columns != expected_columns:
                alignment_errors.append({
                    'sheet': sheet_name,
                    'expected': expected_columns,
                    'actual': actual_columns,
                    'missing': [col for col in expected_columns if col not in actual_columns],
                    'extra': [col for col in actual_columns if col not in expected_columns]
                })
        
        if alignment_errors:
            print("\n" + "="*60)
            print("❌ COLUMN ALIGNMENT ERRORS DETECTED")
            print("="*60)
            
            for error in alignment_errors:
                print(f"\n🔴 {error['sheet']}:")
                print(f"  Expected columns: {error['expected']}")
                print(f"  Actual columns:   {error['actual']}")
                
                if error['missing']:
                    print(f"  Missing columns:  {error['missing']}")
                if error['extra']:
                    print(f"  Extra columns:    {error['extra']}")
            
            print("\n" + "="*60)
            print("SOLUTION: Check your input Excel file for:")
            print("1. Inconsistent header row positions between sheets")
            print("2. Missing or extra columns in some sheets")
            print("3. Data starting in wrong rows/columns")
            print("4. Merged cells or formatting issues")
            print("="*60)
            
            raise ValueError(f"Column alignment validation failed for {len(alignment_errors)} sheets. " +
                           "Please fix the Excel file structure before running analysis.")
        
        print("✅ Column alignment validation passed!")
        return True

    def attempt_column_realignment(self, df, sheet_name):
        """
        Attempt to fix obvious column misalignments by looking for header patterns.
        This is a best-effort approach for common misalignment issues.
        """
        print(f"  Attempting to realign columns for {sheet_name}...")
        
        # Look for datetime columns that might be misaligned
        datetime_patterns = ['time', 'desk', 'intake', 'peer', 'hp', 'finish']
        potential_datetime_cols = []
        
        for col in df.columns:
            if any(pattern in str(col).lower() for pattern in datetime_patterns):
                potential_datetime_cols.append(col)
        
        # If we have potential datetime columns, try to map them properly
        if len(potential_datetime_cols) >= 2:
            print(f"    Found potential datetime columns: {potential_datetime_cols}")
            
            # Try to identify start and end times
            if len(potential_datetime_cols) >= 2:
                df['start_time'] = df[potential_datetime_cols[0]]
                df['end_time'] = df[potential_datetime_cols[-1]]
                
                # Calculate duration
                start_times = pd.to_datetime(df['start_time'], errors='coerce')
                end_times = pd.to_datetime(df['end_time'], errors='coerce')
                raw_durations = (end_times - start_times).dt.total_seconds() / 60
                
                # Validate that the realignment makes sense
                valid_durations = raw_durations[(raw_durations > 5) & (raw_durations < 480)]
                
                if len(valid_durations) > 0:
                    print(f"    ✅ Realignment successful - {len(valid_durations)} valid durations found")
                    return True
                else:
                    print(f"    ❌ Realignment failed - no valid durations found")
                    return False
        
        return False

    def fix_shorthand_dates(self, df, year=2025):
        """Fix shorthand dates like '0101' to proper datetime format."""
        print(f"  Fixing shorthand dates in date column...")
        
        # Store original values for logging
        original_dates = df["date"].copy()
        
        # Convert to string and handle various formats
        df["date"] = df["date"].astype(str)
        
        # Remove any whitespace
        df["date"] = df["date"].str.strip()
        
        # Handle different shorthand formats
        def parse_shorthand_date(date_str):
            if pd.isna(date_str) or date_str in ['nan', 'NaN', '', 'None']:
                return pd.NaT
            
            # Handle "2522 0101" format - extract the date part after space
            date_str = str(date_str).strip()
            if ' ' in date_str:
                parts = date_str.split()
                if len(parts) >= 2:
                    date_str = parts[-1]  # Take the last part (the date)
            
            # Remove any non-digit characters except for common separators
            clean_date = ''.join(c for c in str(date_str) if c.isdigit())
            
            # Handle 4-digit format (MMDD)
            if len(clean_date) == 4:
                try:
                    month = int(clean_date[:2])
                    day = int(clean_date[2:])
                    if 1 <= month <= 12 and 1 <= day <= 31:
                        return pd.to_datetime(f"{year}-{month:02d}-{day:02d}")
                except:
                    pass
            
            # Handle 3-digit format (MDD)
            elif len(clean_date) == 3:
                try:
                    month = int(clean_date[:1])
                    day = int(clean_date[1:])
                    if 1 <= month <= 12 and 1 <= day <= 31:
                        return pd.to_datetime(f"{year}-{month:02d}-{day:02d}")
                except:
                    pass
            
            # Try direct parsing for other formats
            try:
                return pd.to_datetime(date_str, errors='coerce')
            except:
                return pd.NaT
        
        # Apply the parsing function
        df["date"] = df["date"].apply(parse_shorthand_date)
        
        # Log parsing results
        total_dates = len(df)
        valid_dates = df["date"].notna().sum()
        invalid_dates = total_dates - valid_dates
        
        print(f"  Date parsing results: {valid_dates}/{total_dates} valid dates")
        
        if invalid_dates > 0:
            print(f"  WARNING: {invalid_dates} dates could not be parsed")
            # Log the problematic rows
            invalid_mask = df["date"].isna()
            if invalid_mask.any():
                print(f"  Problematic date values:")
                for idx, orig_date in enumerate(original_dates[invalid_mask]):
                    if idx < 5:  # Show first 5 examples
                        print(f"    Row {invalid_mask[invalid_mask].index[idx]}: '{orig_date}'")
                    elif idx == 5:
                        print(f"    ... and {invalid_dates - 5} more")
                        break
        
        return df

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
            
            # Convert Excel serial datetime values to proper timestamps
            # Check each column for Excel serial datetime values (float columns that might be dates)
            for col in df.columns:
                if df[col].dtype in ['float64', 'float32'] and df[col].notna().any():
                    # Check if values look like Excel serial dates (reasonable range)
                    sample_values = df[col].dropna()
                    if len(sample_values) > 0:
                        # Excel serial dates are typically in range 1-100000 (1900-2200s)
                        min_val, max_val = sample_values.min(), sample_values.max()
                        if 1 <= min_val <= 100000 and 1 <= max_val <= 100000:
                            print(f"  Converting column '{col}' from Excel serial to datetime")
                            df[col] = pd.to_datetime(df[col], unit='d', origin='1899-12-30', errors='coerce')
            
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
                lambda row: self.is_interpreter_needed_simple(row.get("language", ""), row.get("Comments", "")), axis=1
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
            
            # Extract only the time portion from start_time and end_time (not the full datetime)
            # This keeps the output clean since we already have a separate date column
            if 'start_time' in df.columns:
                start_datetime = pd.to_datetime(df['start_time'], errors='coerce')
                df['start_time'] = start_datetime.dt.strftime('%I:%M %p')  # Format as "08:56 AM"
            
            if 'end_time' in df.columns:
                end_datetime = pd.to_datetime(df['end_time'], errors='coerce')
                df['end_time'] = end_datetime.dt.strftime('%I:%M %p')  # Format as "09:30 AM"

            # Set up the final column structure
            expected_columns = [
                "date", "start_time", "end_time", "appointment_duration_min",
                "interpreter_needed", "day_of_week", "hour_of_day", "language", "Comments"
            ]
            df = df.reindex(columns=expected_columns)

            # Double-check that we didn't mess up the column order
            if list(df.columns) != expected_columns:
                raise ValueError(f"Column misalignment detected in sheet {sheet} — check sheet format before appending.")

            # Fix shorthand dates like '0101' to proper datetime format
            df = self.fix_shorthand_dates(df, year=2025)

            # Only keep rows that have valid appointment times
            valid_rows = df['appointment_duration_min'].notna()
            df_filtered = df[valid_rows].copy()
            
            print(f"  Final records for {sheet}: {len(df_filtered)}")
            if len(df_filtered) > 0:
                # Add sheet name tracking for better error reporting
                df_filtered['_source_sheet'] = sheet
                processed_frames.append(df_filtered)

        # Combine all the daily sheets into one big dataset
        if processed_frames:
            # 🔍 VALIDATION STEP: Check column alignment before concatenation
            try:
                self.validate_column_alignment(processed_frames)
            except ValueError as e:
                print(f"\n❌ Column alignment validation failed!")
                print(f"Error: {e}")
                
                # Show which sheets were processed
                print(f"\nProcessed sheets:")
                for i, df in enumerate(processed_frames):
                    sheet_name = df['_source_sheet'].iloc[0] if '_source_sheet' in df.columns else f"Sheet_{i+1}"
                    print(f"  - {sheet_name}: {len(df)} records, columns: {list(df.columns)}")
                
                # Don't continue with misaligned data
                self.all_data = pd.DataFrame()
                return
            
            # Remove the temporary source sheet column before concatenation
            for df in processed_frames:
                if '_source_sheet' in df.columns:
                    df.drop('_source_sheet', axis=1, inplace=True)
            
            self.all_data = pd.concat(processed_frames, ignore_index=True)
            
            # Ensure date column contains only date (no time component)
            if 'date' in self.all_data.columns:
                # 1. Convert to datetime using pd.to_datetime()
                self.all_data['date'] = pd.to_datetime(self.all_data['date'], errors='coerce')
                # 2. Use .dt.date to strip off any time (removes '00:00:00')
                self.all_data['date'] = self.all_data['date'].dt.date
                print(f"\nDate column converted to date-only format (no time)")
            
            print(f"All sheets processed! Total valid records: {len(self.all_data)}")
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
        print(f"Interpreter Count (refined): {self.all_data['interpreter_needed'].sum()}")
        
        # Show interpreter detection breakdown
        interpreter_rows = self.all_data[self.all_data['interpreter_needed'] == True]
        print(f"\nINTERPRETER DETECTION SUMMARY:")
        print(f"Total appointments: {len(self.all_data)}")
        print(f"Appointments flagged as needing interpreter: {len(interpreter_rows)}")
        print(f"Percentage needing interpreter: {len(interpreter_rows)/len(self.all_data)*100:.1f}%")
        print("="*50)

    def debug_interpreter_detection(self, sample_size=10):
        """
        Debug method to show examples of interpreter detection results.
        This helps verify the accuracy of the refined detection logic.
        """
        if self.all_data.empty:
            print("No data available for interpreter detection analysis.")
            return
        
        print("\n" + "="*60)
        print("INTERPRETER DETECTION DEBUG ANALYSIS")
        print("="*60)
        
        # Show examples of rows flagged as needing interpreter
        interpreter_rows = self.all_data[self.all_data['interpreter_needed'] == True]
        no_interpreter_rows = self.all_data[self.all_data['interpreter_needed'] == False]
        
        print(f"\n✅ FLAGGED AS NEEDING INTERPRETER (showing first {sample_size}):")
        print("-" * 60)
        
        for idx, row in interpreter_rows.head(sample_size).iterrows():
            lang = row.get('language', 'N/A')
            comments = row.get('Comments', 'N/A')
            duration = row.get('appointment_duration_min', 0)
            print(f"Row {idx}: Duration={duration:.1f}min")
            print(f"  Language: '{lang}'")
            print(f"  Comments: '{comments}'")
            print()
            
        print(f"\n❌ NOT FLAGGED AS NEEDING INTERPRETER (showing first {sample_size}):")
        print("-" * 60)
        
        for idx, row in no_interpreter_rows.head(sample_size).iterrows():
            lang = row.get('language', 'N/A')
            comments = row.get('Comments', 'N/A')
            duration = row.get('appointment_duration_min', 0)
            print(f"Row {idx}: Duration={duration:.1f}min")
            print(f"  Language: '{lang}'")
            print(f"  Comments: '{comments}'")
            print()
            
        print(f"\n📊 SUMMARY STATISTICS:")
        print(f"Total appointments: {len(self.all_data)}")
        print(f"Flagged as needing interpreter: {len(interpreter_rows)} ({len(interpreter_rows)/len(self.all_data)*100:.1f}%)")
        print(f"Not flagged as needing interpreter: {len(no_interpreter_rows)} ({len(no_interpreter_rows)/len(self.all_data)*100:.1f}%)")
        
        if len(interpreter_rows) > 0:
            print(f"Avg duration WITH interpreter: {interpreter_rows['appointment_duration_min'].mean():.1f}min")
        if len(no_interpreter_rows) > 0:
            print(f"Avg duration WITHOUT interpreter: {no_interpreter_rows['appointment_duration_min'].mean():.1f}min")
        
        print("="*60)

    def calculate_normalized_busiest_days(self):
        # Figure out average clients per day, accounting for the fact that
        # some weekdays might appear more often than others in the month
        if self.all_data.empty:
            return pd.DataFrame()
        
        # Use the properly formatted date column to extract weekdays
        # This ensures we get accurate weekday counts from the actual dates
        valid_dates = self.all_data['date'].dropna()
        if valid_dates.empty:
            print("  WARNING: No valid dates found for normalized analysis")
            return pd.DataFrame()
        
        # Extract weekday names from the properly formatted dates
        # Convert back to datetime temporarily for weekday name extraction
        temp_dates = pd.to_datetime(valid_dates)
        weekday_names = temp_dates.dt.day_name()
        
        # Count how many times each weekday appears in our data
        weekday_counts = Counter(weekday_names)
        
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

    def validate_interpreter_detection_final(self):
        """
        Final validation method to help with manual QA testing.
        Shows all unique language values and their flagging status.
        """
        if self.all_data.empty:
            print("No data available for validation.")
            return
        
        print("\n" + "="*60)
        print("FINAL INTERPRETER DETECTION VALIDATION")
        print("="*60)
        
        # Get all unique language values
        unique_languages = []
        for lang in self.all_data['language'].dropna().unique():
            if isinstance(lang, str):
                unique_languages.append(lang)
        
        print(f"\nALL UNIQUE LANGUAGE VALUES ({len(unique_languages)} total):")
        print("-" * 50)
        
        flagged_count = 0
        not_flagged_count = 0
        
        for lang in sorted(unique_languages):
            count = sum(self.all_data['language'] == lang)
            flagged = sum((self.all_data['language'] == lang) & (self.all_data['interpreter_needed'] == True))
            
            if flagged > 0:
                flagged_count += flagged
                print(f"✅ '{lang}': {count} total, {flagged} flagged as needing interpreter")
            else:
                not_flagged_count += count
                print(f"❌ '{lang}': {count} total, 0 flagged as needing interpreter")
        
        print(f"\nSUMMARY:")
        print(f"Total appointments: {len(self.all_data)}")
        print(f"Flagged as needing interpreter: {flagged_count}")
        print(f"Not flagged as needing interpreter: {not_flagged_count}")
        print(f"Percentage needing interpreter: {flagged_count/len(self.all_data)*100:.1f}%")
        
        # Manual verification instructions
        print(f"\n" + "="*60)
        print("MANUAL VERIFICATION CHECKLIST:")
        print("="*60)
        print("1. Review the ✅ flagged languages above:")
        print("   - Are they all actual languages (not appointment notes)?")
        print("   - Are any important languages missing?")
        print("")
        print("2. Review the ❌ not flagged languages above:")
        print("   - Are there any actual languages that should be flagged?")
        print("   - Are the appointment notes correctly excluded?")
        print("")
        print("3. Open the Excel file and manually count:")
        print("   - Filter for Spanish appointments")
        print("   - Filter for Somali appointments") 
        print("   - Filter for any other languages you see")
        print("")
        print("4. Compare manual count with script count:")
        print(f"   - Script found: {flagged_count} appointments needing interpreter")
        print(f"   - Manual count should be close to this number")
        print("="*60)

    def save_results(self, stats, busiest):
        # Save everything to a simple Excel file with multiple sheets (no charts)
        if self.all_data.empty:
            print("No data to save!")
            return
            
        print(f"Saving results to {self.output_file}...")

        # Use pandas ExcelWriter for simple output
        with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
            
            # Sheet 1: All the raw processed data
            if not self.all_data.empty:
                # Remove columns H and I (language and Comments) for cleaner output
                clean_data = self.all_data.drop(columns=['language', 'Comments'], errors='ignore')
                clean_data.to_excel(writer, sheet_name='All Processed Data', index=False)

            # Sheet 2: Summary statistics
            if stats:
                stats_df = pd.DataFrame(list(stats.items()), columns=['Statistic', 'Value'])
                stats_df.to_excel(writer, sheet_name='Summary Statistics', index=False)

            # Sheet 3: Busiest days
            if 'busiest_days' in busiest and busiest['busiest_days']:
                days_df = pd.DataFrame(list(busiest['busiest_days'].items()), columns=['Day', 'Count'])
                day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                days_df['Day'] = pd.Categorical(days_df['Day'], categories=day_order, ordered=True)
                days_df = days_df.sort_values('Day').reset_index(drop=True)
                days_df.to_excel(writer, sheet_name='Busiest Days', index=False)

            # Sheet 4: Busiest hours
            if 'busiest_hours' in busiest and busiest['busiest_hours']:
                hours_df = pd.DataFrame(list(busiest['busiest_hours'].items()), columns=['Hour', 'Count'])
                hours_df = hours_df.sort_values('Hour').reset_index(drop=True)
                hours_df.to_excel(writer, sheet_name='Busiest Hours', index=False)

            # Sheet 5: Interpreter comparison
            if not self.all_data.empty:
                interpreter_data = []
                with_interp = self.all_data[self.all_data['interpreter_needed'] == True]['appointment_duration_min'].dropna()
                without_interp = self.all_data[self.all_data['interpreter_needed'] == False]['appointment_duration_min'].dropna()
                
                if not with_interp.empty:
                    interpreter_data.append(['With Interpreter', with_interp.mean()])
                if not without_interp.empty:
                    interpreter_data.append(['Without Interpreter', without_interp.mean()])
                
                if interpreter_data:
                    interp_df = pd.DataFrame(interpreter_data, columns=['Interpreter Status', 'Average Duration (minutes)'])
                    interp_df.to_excel(writer, sheet_name='Interpreter Comparison', index=False)

            # Sheet 6: Normalized busiest days
            avg_clients_df = self.calculate_normalized_busiest_days()
            if not avg_clients_df.empty:
                avg_clients_df.to_excel(writer, sheet_name='Avg Clients per Weekday', index=False)

        print("Results saved successfully!")

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

        # Show interpreter detection debug information
        self.debug_interpreter_detection(sample_size=5)

        # Run final validation
        self.validate_interpreter_detection_final()

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
        output_file='2522_monthly_analysis.xlsx'
    )
    analyzer.run_analysis()

if __name__ == "__main__":
    main()