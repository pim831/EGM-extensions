import pandas as pd
import os
from ast import literal_eval


def clean_files_in_directory(path):
    files = os.listdir(path)
    for f in files:
        if f.endswith(".csv"):
            df = pd.read_csv(path + f, sep=";")
            df["Description"] = df["Description"].apply(literal_eval)
            unique_lists = []
            for i, d in enumerate(df["Description"].copy()):
                new_list = True
                for l in unique_lists:
                    if set(l) == set(d):
                        df.drop(i, inplace=True)
                        new_list = False
                        break
                if new_list:
                    unique_lists.append(d)
            df.reset_index(drop=True, inplace=True)
            df.to_csv(path + "clean_results/" + f, index=False, sep=";")


# Change to directory for which you want to clean the raw files
path = "Code/OB1/a"

clean_files_in_directory(path)
