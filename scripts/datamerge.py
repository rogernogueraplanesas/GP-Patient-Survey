import sys
import csv
import pandas as pd


FIRST_YEAR = 2018
LAST_YEAR = 2023


def main():
    '''
    Main imports the gp patient survey dataframes, checks for the intersection of the columns
    across all of the dataframes. Imports the metadata, again uses the intersection operator
    to get all of the columns that are the same accross all of the dataframes and the metadata.
    Selects the appropiate columns and then merges them together.
    Finally it renames the columns to have a better description of each column and saves the df as csv.
    '''
    gp_patient_survey_dfs:dict = import_data_frames()
    common_columns_gp_patient_survey:list = common_columns_accross_gp_patient_survey_dfs(gp_patient_survey_dfs)
    gp_metadata:dict = import_metadata()
    gp_metadata_columns:set = set(gp_metadata.keys())
    common_columns_gp_patient_survey_and_metadata = common_columns_gp_patient_survey.intersection(gp_metadata_columns)
    list_common_colums_gp_patients = list(common_columns_gp_patient_survey_and_metadata)
    # Sorts the common columns across all of the data sources according to the metadata
    list_common_colums_gp_patients = sorted(list_common_colums_gp_patients,
                                             key = lambda x: list(gp_metadata.keys()).index(x))
    merged_gp_patient_survey = column_selector_and_union(gp_patient_survey_dfs,
                                                          list_common_colums_gp_patients)
    # Filters the metadata dictionary to include only relevant keys (common columns)
    columns_to_rename:dict = {key: gp_metadata[key] for key in list_common_colums_gp_patients if key in gp_metadata}
    merged_gp_patient_survey_renamed = merged_gp_patient_survey.rename(columns=columns_to_rename)
    merged_gp_patient_survey_renamed.to_csv("cleaned_data/data_merging/gp_patient_survey.csv")

def import_data_frames() -> dict:
    '''
    Import all of the GP Patient survey data as dataframes,
    add a year column and convert to lowercase the columns.
    returns a dictionary of dataframes.
    '''
    files_path:str = "raw_data/GPPS_{}_Practice_data_(weighted)_(csv)_PUBLIC.csv"
    gp_patient_survey_dfs:dict = {}
    for year in range(FIRST_YEAR,LAST_YEAR+1):
        gp_patient_survey_paths:str = files_path.format(year)
        try:
            df = pd.read_csv(gp_patient_survey_paths)
            df["year"] = year
            df.columns = df.columns.str.lower()
            gp_patient_survey_dfs[year] = df
        except pd.errors.FileNotFoundError:
            sys.exit("One of the GP Patient surveys file couldn't be found.")
    return gp_patient_survey_dfs


def common_columns_accross_gp_patient_survey_dfs(gp_patient_survey_dfs) -> list:
    '''
    This function returns a list that contains the intersection (set operator)
    of the columns in the GP Patient survey dataframes
    most of the columns are the same but there has been some changes
    specially in the year 2023
    '''
    columns_gp_patient_survey_dfs = set(gp_patient_survey_dfs[FIRST_YEAR].columns)
    for df_year, df in gp_patient_survey_dfs.items():
        columns_gp_patient_survey_dfs = columns_gp_patient_survey_dfs.intersection(df.columns)
    return columns_gp_patient_survey_dfs


def import_metadata() -> dict:
    '''
    Function to import the metadata, this is useful to rename the columns and make
    sure that all of the columns are consistent. a dict is returned
    containing the column value and the metadata value of the column.
    '''
    metadata = {"year": "year"}
    try:
        with open("raw_data/Metadata.csv", "r") as file:
            reader = csv.reader(file, delimiter=",")
            next(reader)
            for row in reader:
                if len(row) == 2:
                    column_name = row[0].strip().lower()
                    new_column_name = row[1].strip().lower().replace(" ", "_")
                    metadata[column_name] = new_column_name
                else:
                    pass
    except (IOError, OSError, FileNotFoundError):
        sys.exit("Error when processing metadata file.")
    else:
        return metadata


def column_selector_and_union(dict_of_dfs: dict, list_of_selected_cols: list):
    '''
    This function takes a dictionary of dataframes and a list of column names to be selected.
    selects the appropiate columns for each dataframe in the dictionary and then 
    it uses the union operator on all of the dataframes to merge them together into a single df.
    '''
    selected_columns_in_dfs = {}
    for key, df in dict_of_dfs.items():
        selected_cols_df = df.loc[:, list_of_selected_cols]
        selected_columns_in_dfs[key] = selected_cols_df
    union_df = pd.concat(selected_columns_in_dfs.values(), axis=0, ignore_index=True)
    return union_df


if __name__ == "__main__":
    main()
