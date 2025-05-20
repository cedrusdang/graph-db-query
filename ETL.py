from pathlib import Path
import pandas as pd

def extract(file_name: str, drop_na: bool = True) -> pd.DataFrame:
    # Extract the root path
    root_path = Path(__file__).resolve().parent
    # Asembling the path
    csv_path = root_path / "data" / file_name

    # Extract the main data csv file
    data = pd.read_csv(csv_path)

    # Drop rows that contain na if needed
    if drop_na:
        data.dropna()

    return(data)

data = extract("ARDD_Fatalities_Dec_2024.csv")