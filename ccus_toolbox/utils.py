import sys
import os
import requests
import pandas as pd
import numpy as np
import json
import re
from urllib.parse import urlparse
from xhtml2pdf import pisa

# from io import BytesIO

from ..config import TOKEN, DICTIONARIES, CATEGORIES

TEST = "tEST"


def getResponse(url: str, icona=False):
    """
    A refined version of requests.get()
    """
    # un = 'c12e557c-a19b-423a-a5b3-40ea067cae42'
    # pw = 'DQgnBtszFCwyzfz4'
    # auth = requests.auth.HTTPBasicAuth(un,pw)
    auth = TOKEN.CONNECT_AUTH

    if icona:
        headers = {
            "accept": "application/json",
            "icona-auth-key": TOKEN.ICONA_KEY,
            "User-Agent": "PostmanRuntime/7.26.8",
        }
    else:
        headers = {
            "Accept": "application/json",
            # 'Authorization': AUTH,
            "User-Agent": "PostmanRuntime/7.26.8",
        }

    try:
        # response = requests.request("GET", url, headers=headers, params=params)
        if icona:
            response = requests.request(
                "GET", url, headers=headers, timeout=1000, verify=False
            )
        else:
            response = requests.get(
                url=url,
                headers=headers,
                auth=auth,
                stream=True,
                timeout=1000,
                verify=False,
            )
        # return response
        reqIsJson = False
        reqIsPdf = False

        if "application/json" in response.headers.get("content-type"):
            reqIsJson = True

        if "application/pdf" in response.headers.get("content-type"):
            reqIsPdf = True

        if response.status_code == 200 and (reqIsJson or reqIsPdf):
            return response

        if response.status_code == 200 and (reqIsJson and reqIsPdf) == False:
            print(
                "Unsupported content type received : ",
                response.headers.get("content-type"),
            )
            sys.exit()

        print("Status Code: " + str(response.status_code))

        if response.status_code == 400:
            print(
                "The server could not understand your request, check the syntax for your query."
            )
            print("Error Message: " + str(response.json()))
        elif response.status_code == 401:
            print("Login failed, please check your user name and password.")
        elif response.status_code == 403:
            print("You are not entitled to this data.")
        elif response.status_code == 404:
            print(
                "The URL you requested could not be found or you have an invalid view name."
            )
        elif response.status_code == 500:
            print(
                "The server encountered an unexpected condition which prevented it from fulfilling the request."
            )
            print("Error Message: " + str(response.json()))
            print("If this persists, please contact customer care.")
        else:
            print("Error Message: " + str(response.json()))

        sys.exit()

    except Exception as err:
        print("An unexpected error occurred")
        print("Error Message: {0}".format(err))
        sys.exit()


def response_to_dataframe(response):
    """
    convert the returned json data into pandas dataframe
    """
    elements = {"elements", "Elements", "element", "Element"}

    response_json = response.json()
    json_lists = []

    # get the list of the json files
    if isinstance(response_json, list):
        json_lists = response_json
    elif isinstance(response_json, dict):
        # Find if any of the keys match our expected element containers
        matches = list(set(response_json.keys()).intersection(elements))
        if matches:
            json_lists = response_json.get(matches[0])
        else:
            # If no container found, return raw JSON (e.g. for count endpoints)
            return response_json
    else:
        return response_json

    if not json_lists:
        return pd.DataFrame()

    df = pd.DataFrame()
    try:
        # Check if json_lists is indeed a list of dicts/records
        if isinstance(json_lists, list):
            for entry in json_lists:
                s = pd.Series(entry)
                df = pd.concat([df, s], axis=1)
            df = df.T
        else:
            # If it's something else, return the raw data
            return response_json
    except Exception:
        return response_json

    return df


def get_response_in_dataframe(url: str, icona=False):
    response = getResponse(url, icona)
    dataframe = response_to_dataframe(response)

    return dataframe


def filter_projects(
    df,
    exclude_cancelled=False,
    capture_only=False,
    large_projects_only=False,
    storage_projects_only=False,
) -> pd.DataFrame:
    """
    Helper method to filter a dataframe based on project status and type.
    """
    if df is None:
        return None

    filtered_df = df.copy()

    if exclude_cancelled:
        if "status" in filtered_df.columns:
            filtered_df = filtered_df[
                ~filtered_df.status.isin(CATEGORIES.REMOVE_CANCELLED_PROJECT_STATUS)
            ]

    if capture_only:
        if "type" in filtered_df.columns:
            filtered_df = filtered_df[
                filtered_df.type.isin(CATEGORIES.CAP_ONLY_PROJECT_TYPE)
            ]

    if large_projects_only:
        if "size_category" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df.size_category == "Large"]

    if storage_projects_only:
        if "type" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df.type.isin(CATEGORIES.STORAGE_PROJS)]

    return filtered_df


def writeToExcel(
    df: pd.DataFrame, excel_file_path: str, sheet_name: str, index=False
) -> None:
    """
    df: dataframe to be saved
    excel_file_path: the path of the excel file
    sheet_name: the sheet name of the excel file

    Append an df to an existing Excel file
    """
    df.columns = df.columns.astype(str)  # ensure all columns are string
    # Check if the file exists
    dict_data = {}
    if os.path.exists(excel_file_path):
        # if the file already exist, read the files:
        dict_data = pd.read_excel(excel_file_path, sheet_name=None)
        dict_data[sheet_name] = df
    else:
        dict_data[sheet_name] = df

    with pd.ExcelWriter(excel_file_path) as writer:
        for sheet_name, df in dict_data.items():
            df.to_excel(
                writer, sheet_name=sheet_name, startrow=0, startcol=0, index=index
            )
            worksheet = writer.sheets[sheet_name]
            # Define the range of the table (adjusting for Excel's 1-based indexing)
            (max_row, max_col) = df.shape
            column_settings = [{"header": column} for column in df.columns]

            # Add a table to the worksheet
            worksheet.add_table(
                0,
                0,
                max_row,
                max_col - 1,
                {"columns": column_settings, "style": "Table Style Light 1"},
            )
            file = os.path.basename(excel_file_path)
    print(f"{sheet_name} is saved in Excel: {file}")


def retain_first_unique(df):
    if "id" not in df.columns:
        raise KeyError("The dataframe does not have an 'id' column.")
    return df.drop_duplicates(subset="id", keep="first").reset_index(drop=True)


def grouped_by_id_v1(df, id="id"):
    """
    Ensure that each entry_id has only one row.

    For the many side of the one-to-many relationship, zip the many into one, and concatenae them by comma.
    Example:

        ID	Value1	Value2	Value3
        0	1	A	X	1
        1	1	B	Y	2
        2	2	C	Z	3
        3	2	D	W	4
        4	2	E	P	5

    The output will be:

        ID	Value1	Value2	    Value3
        0	1	    A,B	X,Y	    1,2
        1	2	    C,D,E	    Z,W,P	3,4,5

    """
    # ensure the id is integer
    if id in df.columns:
        # df['entry_id']=df['entry_id'].astype(int)
        df.loc[:, id] = df[id].astype(int)

    # conversion dictionary
    convert_to_string = {
        col: "str" for col in df.columns if col not in ["id", "ID", "ROW_ID", id]
    }

    # convert every element of the df into a string, so it can be concatenated without error
    df = df.astype(convert_to_string)

    # Initialize grouped as empty DataFrame
    grouped = pd.DataFrame()

    # considering two types of ids
    if id in df.columns:
        # df.loc[:,'id']=df.id.astype(int) # convert the id back to int
        grouped = df.groupby(id).agg(lambda x: ",".join(x)).reset_index()
    elif "ID" in df.columns:
        # df.loc[:,'id']=df.id.astype(int) # convert the id back to int
        grouped = df.groupby("ID").agg(lambda x: ",".join(x)).reset_index()
    elif "id" in df.columns:
        # df.loc[:,'id']=df.id.astype(int) # convert the id back to int
        grouped = df.groupby("id").agg(lambda x: ",".join(x)).reset_index()
    else:
        print("The dataframe does not have id")

    return grouped


def grouped_by_id(df, id="id"):
    """
    Ensure that each entry_id has only one row by concatenating values with commas.
    Handles all data types safely by explicitly converting to strings first.

    Args:
        df: pandas DataFrame to process
        id: column name to use as grouping key (default "id")

    Returns:
        DataFrame with one row per unique ID and concatenated values
    """

    def safe_str(x):
        """Helper function to safely convert values to strings, handling nulls and special cases."""
        if pd.isna(x) or x is None:
            return ""
        if isinstance(x, (np.integer, np.floating)):
            return str(int(x) if x.is_integer() else x)
        return str(x)

    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame")

    # Create a copy to avoid modifying the original DataFrame
    df = df.copy()

    # Convert all non-ID columns to strings explicitly
    non_id_cols = [col for col in df.columns if col not in [id]]

    # First convert entire columns to string dtype to avoid FutureWarning
    df[non_id_cols] = df[non_id_cols].astype(str)

    # Then apply safe_str to handle nulls and special cases
    for col in non_id_cols:
        df.loc[:, col] = df[col].map(safe_str)

    def safe_join(series):
        """Helper function to safely join series values with commas."""
        try:
            return ",".join(str(x) for x in series if pd.notna(x) and str(x).strip())
        except Exception as e:
            print(f"Join error on column {series.name}: {e}")
            return ""

    # Group and concatenate values with error handling
    try:
        if id in df.columns:
            return df.groupby(id).agg(safe_join).reset_index()
        elif "ID" in df.columns:
            return df.groupby("ID").agg(safe_join).reset_index()
        elif "id" in df.columns:
            return df.groupby("id").agg(safe_join).reset_index()
        else:
            raise ValueError("DataFrame does not have a valid ID column")
    except Exception as e:
        print(f"Error during grouping: {e}")
        # Return original df if grouping fails
        return df


def unpivot_column(df: pd.DataFrame, column_name: str, seperator: str = ","):
    """
    unpivot a column based on the seperator
    """

    if column_name not in df.columns:
        raise KeyError(f"{column_name} not found in the dataframe.")

    column_splited = (
        df.get(column_name, "")
        .str.split(seperator, expand=True)
        .stack()
        .reset_index(level=1, drop=True)
    )
    df_splited = pd.DataFrame(column_splited).rename(
        columns={0: f"{column_name} unpivoted"}
    )
    df_new = df.merge(df_splited, left_index=True, right_index=True, how="left")

    return df_new


def convert_to_mt(capacity):
    return round(capacity / 1000000, 4)


def convert_to_kt(capacity):
    return round(capacity / 1000, 2)


def multiple_join(series):
    values = set(series.apply(str))
    return ",".join(values)


def get_city_from_coordinates(latitude, longitude):

    api_key = r"pk.681ac3252cdd9d310bbe15fac35e6e12"
    url = f"https://us1.locationiq.com/v1/reverse.php"
    params = {"key": api_key, "lat": latitude, "lon": longitude, "format": "json"}
    response = requests.get(url, params=params, verify=False)
    data = response.json()

    if "address" in data:
        return data["address"].get("city", None)

    return None


def get_json_string(text):
    """
    Extract the json string from the answers of gemini
    """
    start_index = text.find("{")
    end_index = text.rfind("}")
    if start_index != -1:
        substring = text[start_index : end_index + 1]
    else:  # if no json string in the answer, use the default value
        substring = ""

    return substring


def convert_table_to_mt(df):
    """
    Divides all numeric columns in a DataFrame by 1 million.

    Args:
        df: The input pandas DataFrame.

    Returns:
        A new DataFrame with the numeric columns divided, or the original
        DataFrame if no numeric columns are found. Returns None if the input is not a DataFrame.
    """
    if not isinstance(df, pd.DataFrame):
        print("Input must be a pandas DataFrame.")
        return None

    numeric_cols = df.select_dtypes(include=np.number).columns

    if numeric_cols.empty:  # Check if there are any numeric columns
        print("No numeric columns found in the DataFrame.")
        return df  # Return original df if no numeric columns are found

    df_divided = df.copy()  # Create a copy to avoid modifying the original DataFrame
    df_divided[numeric_cols] = df_divided[numeric_cols] / 1000000
    return df_divided


def extract_json_string(text):
    """Extracts the first valid JSON string using a stack."""
    start = text.find("{")
    last = text.find("}")

    results = text[start : last + 1]
    results = results.replace("\n", "")

    return results


def sanitize_filename(filename, replacement="_"):
    """
    Sanitizes a filename by removing or replacing invalid characters.

    Args:
        filename (str): The filename to sanitize.
        replacement (str): The character to replace invalid characters with.

    Returns:
        str: The sanitized filename.
    """

    # Characters not allowed in most filesystems
    invalid_chars = r'[<>:"/\\|?*\x00-\x1F]'

    # Reserved names (Windows)
    reserved_names = [
        "CON",
        "PRN",
        "AUX",
        "NUL",
        "COM1",
        "COM2",
        "COM3",
        "COM4",
        "LPT1",
        "LPT2",
        "LPT3",
        "LPT4",
        "COM0",
        "LPT0",  # some systems have these too.
    ]

    # Replace invalid characters
    sanitized_filename = re.sub(invalid_chars, replacement, filename)

    # Remove trailing dots and spaces
    sanitized_filename = sanitized_filename.rstrip(" .")

    # Handle reserved names (case-insensitive)
    if sanitized_filename.upper() in reserved_names:
        sanitized_filename = replacement + sanitized_filename

    # Limit filename length (optional, depends on filesystem)
    max_length = 255  # Common limit, but can vary
    if len(sanitized_filename) > max_length:
        sanitized_filename = sanitized_filename[:max_length]

    return sanitized_filename


def webpage_download_and_save(url, file_name, output_dir="."):
    """
    Downloads content from a URL and saves it as a file (PDF or original).

    Args:
        url (str): The URL to download.
        file_name (str): The desired file name.
        output_dir (str): The directory to save the file.
    """
    try:
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Ensure the file name ends with .pdf
        if not file_name.lower().endswith(".pdf"):
            file_name += ".pdf"

        # Construct full file path
        filepath = os.path.join(output_dir, file_name)

        # Download content
        response = requests.get(url, headers=DICTIONARIES.HEADERS, stream=True)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "")

        if not os.path.exists(filepath):  # if the file is not already downloaded
            if "application/pdf" in content_type:
                # Save PDF files directly
                with open(filepath, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                print(f"PDF saved to: {filepath}")

            elif "text/html" in content_type:
                # Read HTML content
                html_content = response.content

                # Convert HTML to PDF using weasyprint
                try:
                    from weasyprint import HTML

                    HTML(string=html_content.decode("utf-8")).write_pdf(filepath)
                    print(f"Webpage converted to PDF and saved to: {filepath}")
                except Exception as e:
                    print(f"Error converting webpage to PDF: {e}")
                    # Clean up partially created file
                    if os.path.exists(filepath):
                        os.remove(filepath)

            else:
                # Save other file types
                with open(filepath, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                print(f"File saved to: {filepath}")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def column_summary(data: dict):
    """
    Provide a summary of each column in the dataframe, including data type, number of unique values, and sample values.
    """
    summary_dfs = pd.DataFrame()
    tabs = list(data.keys())
    for tab in tabs:
        df = data[tab]
        summary = []
        for col in df.columns:
            col_data = df[col]
            data_type = col_data.dtype
            num_unique = col_data.nunique(dropna=True)
            sample_values = col_data.dropna().unique()[
                :5
            ]  # Get up to 5 unique sample values
            summary.append(
                {
                    "Column Name": col,
                    "Data Type": str(data_type),
                    "Num Unique Values": num_unique,
                    "Sample Values": ", ".join(map(str, sample_values)),
                }
            )
        summary_df = pd.DataFrame(summary)
        summary_df["data_category"] = tab
        summary_dfs = pd.concat([summary_dfs, summary_df], axis=0)
        summary_dfs = summary_dfs[
            ~summary_dfs["data_category"].isin(["summary", "company", "location"])
        ]
    return summary_dfs


def save_data_to_csv_files(data: dict, output_dir: str):
    """
    Save each dataframe in the data dictionary to a separate CSV file.
    """
    os.makedirs(
        output_dir, exist_ok=True
    )  # Ensure output directory exists, if it exists, do nothing
    for tab_name, df in data.items():
        sanitized_tab_name = sanitize_filename(tab_name)
        file_path = os.path.join(output_dir, f"{sanitized_tab_name}.csv")
        df.to_csv(file_path, index=False)
        print(f"Saved {tab_name} to {file_path}")
