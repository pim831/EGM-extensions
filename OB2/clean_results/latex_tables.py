import pandas as pd
import os
from ast import literal_eval


def clean_files_in_directory(path):
    files = os.listdir(path)
    gestalt_measurements = pd.DataFrame(
        columns=[
            "Event",
            "Top Gestalt",
            "Top 5 Gestalt",
            "Top 10 Gestalt",
            "Top 50 Gestalt",
            "#Unique results",
        ]
    )
    for f in files:
        if f.endswith(".csv"):
            df = pd.read_csv(path + f, sep=";")
            df["Description"] = df["Description"].apply(literal_eval)
            unique_results = df.shape[0]
            counts = [0, 4, 9, 49]
            data = [f.split("_")[0]]
            for q in counts:
                if q < unique_results:
                    data.append(float(df["Quality"][q].split(",")[0][1:]))
                else:
                    data.append(0)
            data.append(unique_results)
            gestalt_measurements.loc[len(gestalt_measurements)] = data
    gestalt_measurements = gestalt_measurements.round(3)
    gestalt_measurements.set_index("Event", inplace=True)
    print(
        gestalt_measurements.style.to_latex(
            label="OB2_gestalt_measurements",
            caption="Gestalt measurements for soccer match events",
        )
    )


clean_files_in_directory("Code/OB2/clean_results/")
