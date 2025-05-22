print("Please ensure that requirements.txt is already been installed with setup_env.sh")
import os
from pathlib import Path
import pandas as pd

# EXTRACT #

def extract(file_name: str, drop_na: bool = True) -> pd.DataFrame:
    print("Begin csv files extraction...")
    # Extract the root path
    root_path = Path(__file__).resolve().parent
    # Asembling the path
    csv_path = root_path / "ELT_input" / file_name

    # Extract the main data csv file
    data = pd.read_csv(csv_path)

    # Drop rows that contain na if needed
    if drop_na:
        data = data.dropna()
    print("Finish extraction! ")
    return(data)

# TRANSFORM #

def transform_column_names(df:pd.DataFrame):
    print("Begin transform columns' names...")
    # Change the name of the orginal columns
    rename_map = {
        "ID": "fatality_sk",
        "Crash ID": "crash_id",
        "State": "state",
        "Month": "month",
        "Year": "year",
        "Dayweek": "dayweek",
        "Time": "time",
        "Crash Type": "crash_type",
        "Bus Involvement": "bus",
        "Heavy Rigid Truck Involvement": "heavy_rigid_truck",
        "Articulated Truck Involvement": "articulated_truck",
        "Speed Limit": "speed_limit",
        "Road User": "road_user",
        "Gender": "gender",
        "Age": "age",
        "National Remoteness Areas": "nra",
        "SA4 Name 2021": "sa4",
        "National LGA Name 2024": "lga",
        "National Road Type": "road_type",
        "Christmas Period": "christmas",
        "Easter Period": "easter",
        "Age Group": "age_group",
        "Day of week": "day_of_week",
        "Number Fatalities":"fatalities",
        "Time of day":"time_of_day",
    }
    print("Finish transform columns' names!")
    return df.rename(columns=rename_map)

def transform_time_to_hour(data, col_name = 'time'):
    print("Begin transform time column to hour...")
    # Left only hour instead of hh:mm format
    data['hour'] = data[col_name].str.split(":").str[0].astype(int)
    print("Finish transform time column to hour!")
    return data

def fix_time_of_day_by_hour(data):
    # Day: 6 <= hour < 18, Night: otherwise.
    print("Fixing time_of_day column based on hour...")
    data['time_of_day'] = data['hour'].apply(lambda h: 'Day' if 6 <= h < 18 else 'Night')
    print("Finished fixing time_of_day column.")
    return data

def create_table(data: pd.DataFrame, cols_sets: dict, table_name:str,
                  sk:bool = True) -> pd.DataFrame:
    print(f"Begin create table {table_name}...")
    # Tranformation to create Dim and fact nodes
    cols = cols_sets[table_name]
    # Extract columns from the data frame and reset the index
    df_table = data[cols].drop_duplicates().sort_values(cols[0]).reset_index(
        drop=True)
    # sk enable pipeline
    if sk:
        # Create sk keys
        df_table = df_table.reset_index(drop=False).rename(columns={
            "index": f"{table_name}_sk"})
        df_table.iloc[:, 0] = df_table.iloc[:, 0] + 1
        # Merge dim table to data table to add sk
        data = data.merge(df_table, on=cols, how='left')
    print(f"Finish create table {table_name}!")
    return df_table, data

def create_rel_load(data, rel_schema):
    rel_tables = {}
    for rule in rel_schema.values():
        rel, src_label, src_key, dst_label, dst_key = rule
        rel_df = data[[src_key, dst_key]].dropna().copy()
        rel_df.rename(columns={src_key: "src_value", dst_key: "dst_value"}, inplace=True)
        rel_tables[rel] = {
            "df": rel_df,
            "src_label": src_label,
            "src_key": src_key,
            "dst_label": dst_label,
            "dst_key": dst_key
        }
        save_to_csv(rel_df, rel)
    return rel_tables

# LOAD
def save_to_csv(table, filename):
    # To save csv
    folder = "ELT_output"
    os.makedirs(folder, exist_ok=True) # Create folder if not exist
    filename = filename + ".csv" # Name the file
    path = os.path.join(folder, filename)
    table.to_csv(path, index=False)
    print(f"Save to: {path}")

def create_table_load(data: pd.DataFrame, cols_sets: dict, table_name:str, sk:bool = True):
    # Create table:
    dim_df, data = create_table(data, cols_sets, table_name, sk)
    # Change all cols to string to ensure MATCH will work
    dim_df = dim_df.astype(str)
    save_to_csv(dim_df, table_name)
    return data

def create_table_load_LGA(data: pd.DataFrame, cols_sets: dict, table_name:str, sk:bool = True):
    # NOTE: This is only for LGA as there is mutiple LGA in different SA4 and State
    ## Create table:
    dim_df, data = create_table(data, cols_sets, table_name, sk)
    # Change all cols to string to ensure MATCH will work
    dim_df = dim_df.astype(str)
    dim_df = dim_df[["lga_sk","lga"]]
    save_to_csv(dim_df, table_name)
    return data

##############################################################################

if __name__ == "__main__":

    # Extract 
    print("\033[92mBegin the ETL process...\033[0m")
    data = extract("ARDD_Fatalities_Dec_2024.csv")

    # NODES TRANSFORM AND LOAD (SAVE TO READY TO LOAD CSV)#
    data = transform_column_names(data) # Rename cols in orginal data
    data = transform_time_to_hour(data)  # Create hour and drop time
    data = fix_time_of_day_by_hour(data) # Fix the time_of_day col

    # Dims and columns sets
    tables_cols_sets = {
        "date": ["dayweek", "day_of_week", "month", "year", ],
        "crash_type": ["crash_type"],
        "holiday": ["easter", "christmas"],
        "sa4": ["sa4"],
        "nra": ["nra"],
        "state": ["state"],
        "gender": ["gender"],
        "age_group": ["age_group"],
        "road_user": ["road_user"],
        "involvement": ["bus", "heavy_rigid_truck", "articulated_truck"],
        "road_type": ["road_type"],
        "speed_limit": ["speed_limit"],
        "time": ["hour", "time_of_day"],
        "crash": ["crash_id","fatalities"],
        "fatality": ["fatality_sk", "age"],
        "lga": ["lga","sa4","state"],
    }
    
    # Create dim tables and save them
    dim_names = [
        "date", "crash_type", "holiday", "sa4", "nra", "state",
        "gender", "age_group", "road_user", "involvement", "road_type",
        "speed_limit", "time"
    ]

    for name in dim_names:
        data = create_table_load(data, tables_cols_sets, name) 
    # NOTE: FOR LGA ONLY
    data = create_table_load_LGA(data, tables_cols_sets, "lga")

    # Load the fact nodes (sk and no sk)
    data = create_table_load(data, tables_cols_sets, "crash")
    data = create_table_load(data, tables_cols_sets, "fatality", sk=False)

    rel_schema = {
        # Location
        1:  ["HAS_LGA",        "state",     "state_sk",      "lga",          "lga_sk"],
        2:  ["IN_SA4",         "lga",       "lga_sk",        "sa4",          "sa4_sk"],
        3:  ["IN_STATE",       "sa4",       "sa4_sk",        "state",        "state_sk"],


        # Crash fact & dimension
        4:  ["IN_NRA",         "crash",     "crash_sk",      "nra",          "nra_sk"],
        5:  ["IN_LGA",         "crash",     "crash_sk",      "lga",          "lga_sk"],
        6:  ["IN_DATE",        "crash",     "crash_sk",      "date",         "date_sk"],
        7:  ["CRASH_TYPE_IS",  "crash",     "crash_sk",      "crash_type",   "crash_type_sk"],
        8:  ["IN_EVENT",       "crash",     "crash_sk",      "holiday",      "holiday_sk"],
        9:  ["IN_ROAD_TYPE",   "crash",     "crash_sk",      "road_type",    "road_type_sk"],
        10: ["IN_SPEED_LIMIT", "crash",     "crash_sk",      "speed_limit",  "speed_limit_sk"],
        11: ["INVOLVEMENT_IS", "crash",     "crash_sk",      "involvement",  "involvement_sk"],
        12: ["IN_TIME",        "crash",     "crash_sk",      "time",         "time_sk"],

        # Fatality fact & dimension
        13: ["IN_CRASH",       "fatality",  "fatality_sk",   "crash",        "crash_sk"],
        14: ["AGE_IS",         "fatality",  "fatality_sk",   "age_group",    "age_group_sk"],
        15: ["GENDER_IS",      "fatality",  "fatality_sk",   "gender",       "gender_sk"],
        16: ["ROAD_USER_IS",   "fatality",  "fatality_sk",   "road_user",    "road_user_sk"],
    }

    create_rel_load(data, rel_schema) # Create the rel and load