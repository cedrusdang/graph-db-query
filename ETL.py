print("Please ensure that requirements.txt is already been installed with setup_env.sh")
import os
from pathlib import Path
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv
from tqdm import tqdm
import time

# EXTRACT #

def extract(file_name: str, drop_na: bool = True) -> pd.DataFrame:
    print("Begin csv files extraction...")
    # Extract the root path
    root_path = Path(__file__).resolve().parent
    # Asembling the path
    csv_path = root_path / "data" / file_name

    # Extract the main data csv file
    data = pd.read_csv(csv_path)

    # Drop rows that contain na if needed
    if drop_na:
        data = data.dropna()
    print("Finish extraction! ")
    return(data)

# TRANSFORM #

def save_to_csv(table, filename):
    # To save csv
    folder = "csv_files_before_load"
    os.makedirs(folder, exist_ok=True) # Create folder if not exist
    filename = filename + ".csv" # Name the file
    path = os.path.join(folder, filename)
    table.to_csv(path, index=False)
    print(f"Save to: {path}")

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
    # Drop the old time column
    data = data.drop(columns=["time"])
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
        # Drop the orginal cols
        data = data.drop(columns=cols)
    print(f"Finish create table {table_name}!")
    return df_table, data



def extract_rel_tables(data, rel_schema, save_csv=True):
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
        if save_csv and not rel_df.empty:
            save_to_csv(rel_df, rel)   # <--- Use your save_to_csv here!
    return rel_tables

# LOAD

def setup_neo4j():
    print("Begin create NEO4J connection...")
    # Setup the env
    print("Loading secret keys from .env...")
    load_dotenv()
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")

    # Connect to local NEO4J only, pls use different codes for cloud NEO4J
    print("Connect to local Graph Data base...")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    print("Finish create NEO4J connection!")
    return driver

def load_table(dim_df, table_name, driver, batch_size=1000):
    """
    Batch load a DataFrame as nodes into Neo4j.
    """
    col_names = dim_df.columns.tolist()
    rows = dim_df.to_dict("records")
    with driver.session() as session:
        for i in tqdm(range(0, len(rows), batch_size), desc=f"Loading nodes of {table_name} to NEO4J"):
            batch = rows[i:i + batch_size]
            props = ', '.join([f"{col}: row.{col}" for col in col_names])
            cypher = f"""
            UNWIND $batch as row
            MERGE (n:{table_name} {{{props}}})
            """
            session.run(cypher, batch=batch)

def load_relationships(rel_tables, driver, batch_size=1000):
    with driver.session() as session:
        for rel, info in rel_tables.items():
            df = info["df"]
            src_label = info["src_label"]
            src_key = info["src_key"]
            dst_label = info["dst_label"]
            dst_key = info["dst_key"]
            rows = df.to_dict("records")
            # Process in batches to avoid memory or query size issues
            for i in tqdm(range(0, len(rows), batch_size), desc=f"Loading {rel}"):
                batch = rows[i:i+batch_size]
                cypher = f"""
                UNWIND $batch as row
                MATCH (a:{src_label} {{{src_key}: row.src_value}})
                MATCH (b:{dst_label} {{{dst_key}: row.dst_value}})
                MERGE (a)-[:{rel}]->(b)
                """
                session.run(cypher, batch=batch)

# CONNECTOR FROM TRANSFORM TO LOAD #

# NOTE: To save RAM, every completed dimension, fact and relationship tables 
# with be loaded and droped write always 

# From Transformation to Loading Pipeline
def create_table_load(data: pd.DataFrame, cols_sets: dict, table_name:str, driver,
                       sk:bool = True, save_csv:bool = True, batch_size: int = 1000):
    # To create table and then load to Neo4J
    ## Create table:
    dim_df, data = create_table(data, cols_sets, table_name, sk)

    ## Change all cols to string to ensure MATCH will work
    dim_df = dim_df.astype(str)

    ## Save to csv files
    if save_csv:
        save_to_csv(dim_df, table_name)
    ## Load to Neo4J
    load_table(dim_df, table_name, driver, batch_size=batch_size)
    return data

def create_rel_load(data, driver, rel_schema, save_csv=True, batch_size=1000):
    # Extract relationship tables and optionally save as CSV
    rel_tables = extract_rel_tables(data, rel_schema, save_csv=save_csv)
    # Batch load relationships into Neo4j
    load_relationships(rel_tables, driver, batch_size=batch_size)

##############################################################################

if __name__ == "__main__":
    ###########
    # Extract #
    ###########

    start_time = time.time()
    print("\033[92mBegin the ETL process...\033[0m")

    data = extract("ARDD_Fatalities_Dec_2024.csv")

    ############################
    # NODES TRANSFORM AND LOAD #
    ############################

    data = transform_column_names(data) # Rename cols in orginal data

    data = transform_time_to_hour(data)  # Create hour and drop time
    data = fix_time_of_day_by_hour(data) # Fix the time_of_day col

    driver = setup_neo4j() # NEO4J environment setup

    # Dims and columns  sets
    tables_cols_sets = {
        "date": ["dayweek", "day_of_week", "month", "year", ],
        "crash_type": ["crash_type"],
        "holiday": ["easter", "christmas"],
        "lga": ["lga"],
        "sa4": ["sa4"],
        "nra": ["nra"],
        "state": ["state"],
        "gender": ["gender"],
        "age_group": ["age_group"],
        "road_user": ["road_user"],
        "involvement": ["bus", "heavy_rigid_truck", "articulated_truck"],
        "road_type": ["road_type"],
        "speed_limit": ["speed_limit"],
        "time": ["hour"],
        "crash": ["crash_id","fatalities"],
        "fatality": ["fatality_sk"]
    }

    # All dims with sk
    dim_names = [
        "date", "crash_type", "holiday", "lga", "sa4", "nra", "state",
        "gender", "age_group", "road_user", "involvement", "road_type"
    ]
    # All dims without sk
    dim_names_no_sk = {"speed_limit", "time"}

    # Create and Load the dim nodes
    for name in dim_names:
        data = create_table_load(data, tables_cols_sets, name, driver, sk=True)
    for name in dim_names_no_sk:
        data = create_table_load(data, tables_cols_sets, name, driver, sk=False)
    # Load the fact nodes
    data = create_table_load(data, tables_cols_sets, "crash", driver, sk=True)
    data = create_table_load(data, tables_cols_sets, "fatality", driver, sk=False)

    ####################################
    # RELATIONSHIPS TRANSFORM AND LOAD #
    ####################################

    # Declare relationship schema

    rel_schema = {
        1: ["IN_DATE", "crash", "crash_sk", "date", "date_sk"],
        2: ["CLASSIFIED_AS", "crash", "crash_sk", "crash_type", "crash_type_sk"],
        3: ["IN_EVENT", "crash", "crash_sk", "holiday", "holiday_sk"],
        4: ["IN_LOCATION", "crash", "crash_sk", "lga", "lga_sk"],
        5: ["IN_SA4", "lga", "lga_sk", "sa4", "sa4_sk"],
        6: ["IN_NRA", "sa4", "sa4_sk", "nra", "nra_sk"],
        7: ["IN_STATE", "nra", "nra_sk", "state", "state_sk"],
        8: ["GENDER_IS", "fatality", "fatality_sk", "gender", "gender_sk"],
        9: ["AGE_IS", "fatality", "fatality_sk", "age_group", "age_group_sk"],
        10: ["ROAD_USER_IS", "fatality", "fatality_sk", "road_user", "road_user_sk"],
        11: ["IN_SPEED_LIMIT", "crash", "crash_sk", "speed_limit", "speed_limit"],
        12: ["DIED_IN", "fatality", "fatality_sk", "crash", "crash_sk"],
        13: ["INVOLVE_WITH", "crash", "crash_sk", "involvement", "involvement_sk"],
        14: ["IN_ROAD_TYPE", "crash", "crash_sk", "road_type", "road_type_sk"],
        15: ["IN_TIME", "crash", "crash_sk", "time", "hour"]
    }

    create_rel_load(data, driver, rel_schema) # Create the rel and load

    driver.close() # Close the NEO4J connection

    # Runtime report
    end_time = time.time()
    print(f"--- Total runtime: {end_time - start_time:.2f} seconds ---")