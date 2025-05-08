import streamlit as st
import polars as pl
import pandas as pd
import re
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import tempfile

# Define the desired sorting structure
SORTING_ORDER = [
    "Admin Early",
    "Admin Late",
    "Admin Night",
    "QA Night",
    "",  # Space
    "QA Resulting Early",
    "QA Resulting Late",
    "",  # Space
    "Resulting Day",
    "Resulting Late",
    "Resulting Night",
    "",  # Space
    "SPECIAL BETS RESULTING EARLY",
    "SPECIAL BETS RESULTING LATE",
    "SPECIAL BETS RESULTING NIGHT",
    "",  # Space
    "QA Live Early",
    "QA Live Late",
    "",  # Space
    "Live Early",
    "Live Day",
    "Live Late",
    "Live Night",
    "",  # Space
    "Training Day Live",
    "Training Day Resulting",
    "", # Space
    "PRODUCTION SUPPORT EARLY",
    "PRODUCTION SUPPORT LATE",
    "PRODUCTION SUPPORT NIGHT",
    ""  # Space
]

# Mapping dictionary for term replacements
TERM_MAPPING = {
    "PRODUCTION SUPPORT EARLY": "PS Early",
    "PRODUCTION SUPPORT LATE": "PS Late",
    "PRODUCTION SUPPORT NIGHT": "PS Night",
    "SPECIAL BETS RESULTING EARLY": "SB Early",
    "SPECIAL BETS RESULTING LATE": "SB Late",
    "SPECIAL BETS RESULTING NIGHT": "SB Night",
    "TRAINING DAY LIVE": "TT Live",
    "TRAINING DAY RESULTING": "TT Resulting",
    "Adminstration": "A DE",
    "Production Service Day": "PSD",
    "Special Task/ Live Backup Late": "ST DE",
}

# Streamlit UI
st.title("Daily TMS Schedule Processor")
st.subheader("Upload Excel file")

# Time adjustment settings
time_adjustment = st.radio(
    "Time Adjustment Mode:",
    options=["DST (-2 hours)", "Non-DST (-1 hour)"],
    index=0
)
hours_to_adjust = -2 if time_adjustment == "DST (-2 hours)" else -1

uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx"])

def adjust_time(time_str, hours=hours_to_adjust):
    """Adjust time by specified hours"""
    try:
        time_obj = datetime.strptime(time_str, "%H:%M")
        adjusted = time_obj + timedelta(hours=hours)
        return adjusted.strftime("%H:%M")
    except:
        return time_str

def split_at_first_number(text):
    """Split text at first occurrence of a time pattern (HH:MM)"""
    if not text:
        return ("", "")
    match = re.search(r'(\d{1,2}:\d{2})', str(text))
    if match:
        split_pos = match.start()
        return (text[:split_pos].strip(), text[split_pos:].strip())
    return (text, "")

def format_time_range(time_str):
    """Format and adjust times into HH:MM - HH:MM format"""
    if not time_str:
        return ""
    
    times = re.findall(r'(\d{2}:\d{2})', str(time_str))
    if len(times) >= 2:
        return f"{adjust_time(times[0])} - {adjust_time(times[1])}"
    elif times:
        return adjust_time(times[0])
    return ""

def custom_sort(value):
    """Custom sorting function based on predefined order"""
    try:
        return SORTING_ORDER.index(value)
    except ValueError:
        return len(SORTING_ORDER)  # Put unspecified values at the end

def map_terms(value):
    """Replace terms according to the mapping dictionary"""
    return TERM_MAPPING.get(value.upper(), value)

if uploaded_file:
    with st.spinner("Processing file..."):
        # Read and process data
        df = pl.read_excel(uploaded_file)
        df_processed = (
            df.slice(7, None)
            .select(pl.all().exclude([df.columns[0], df.columns[2]]))
        )

        def make_unique_names(header_row):
            counts = defaultdict(int)
            unique_headers = []
            for header in header_row:
                if header is None:
                    header = "Colleague"
                header = str(header)
                counts[header] += 1
                if counts[header] > 1:
                    header = f"{header}.{counts[header]-1}"
                unique_headers.append(header)
            return unique_headers

        header_row = make_unique_names(df_processed.row(0))
        df_final = (
            df_processed.rename(dict(zip(df_processed.columns, header_row)))
            .slice(1, None)
            .drop_nulls()
        )

        # Generate UTC timestamp for filename
        utc_now = datetime.now(timezone.utc)
        timestamp_str = utc_now.strftime("%Y-%m-%d-%H-%M")
        output_filename = f"Daily-{timestamp_str}.xlsx"
        
        # Create a temporary file for output
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
            output_path = tmp.name
            
            # Process each column sheet
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Process main sheet
                df_final_pd = df_final.to_pandas()
                df_final_pd.to_excel(writer, sheet_name="Cleaned_Shifts", index=False)
                
                for col in df_final.columns:
                    if col != "Colleague":
                        sheet_df = (
                            df_final.select(["Colleague", col])
                            .drop_nulls()
                            .filter(~pl.col(col).is_in(["r", "rw", "u"]))
                            .with_columns(pl.col(col).str.replace("Europe/Gera", ""))
                            .with_columns(pl.col(col).str.replace("DEG", ""))
                        )
                        
                        # Process each row to split and format
                        processed_rows = []
                        for row in sheet_df.iter_rows(named=True):
                            desc, time = split_at_first_number(row[col])
                            mapped_desc = map_terms(desc)
                            time_range = format_time_range(time)
                            processed_rows.append({
                                "Shift time": time_range,
                                "Colleague": row["Colleague"],
                                col: mapped_desc
                            })
                        
                        # Create new DataFrame with processed data
                        sheet_df = pl.DataFrame(processed_rows)
                        
                        # Apply custom sorting for the second column
                        if col in sheet_df.columns:
                            sheet_df_pd = sheet_df.to_pandas()
                            sheet_df_pd['sort_key'] = sheet_df_pd[col].apply(custom_sort)
                            sheet_df_pd = sheet_df_pd.sort_values('sort_key').drop('sort_key', axis=1)
                            
                            # Insert empty rows where specified in SORTING_ORDER
                            sorted_df = pd.DataFrame()
                            
                            for i, item in enumerate(SORTING_ORDER):
                                if item == "":
                                    empty_row = {"Shift time": "", "Colleague": "", col: ""}
                                    sorted_df = pd.concat([sorted_df, pd.DataFrame([empty_row])], ignore_index=True)
                                else:
                                    mapped_item = map_terms(item)
                                    matches = sheet_df_pd[sheet_df_pd[col] == mapped_item]
                                    sorted_df = pd.concat([sorted_df, matches], ignore_index=True)
                            
                            # Add any remaining rows
                            remaining = sheet_df_pd[~sheet_df_pd[col].isin([map_terms(x) for x in SORTING_ORDER if x])]
                            sorted_df = pd.concat([sorted_df, remaining], ignore_index=True)
                            
                            # Reorder columns
                            sorted_df = sorted_df[["Shift time", "Colleague", col]]
                            
                            # Write to Excel
                            sheet_name = str(col)[:31]
                            sorted_df.to_excel(writer, sheet_name=sheet_name, index=False)

            # Provide download link
            with open(output_path, "rb") as f:
                st.success("Processing complete!")
                st.download_button(
                    label="Download Processed File",
                    data=f,
                    file_name=output_filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                
            # Show preview
            st.subheader("Preview of Processed Data")
            st.dataframe(sorted_df.head(10))