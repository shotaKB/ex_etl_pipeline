import pathlib
import pandas as pd

def collect_files(source):
    # Find all files from source
    paths = list(pathlib.Path().glob(source["pattern"]))
    return paths

def combine_dataframes(source, paths):
    encoding = source.get("encoding", "utf-8")
    dtype_map = source.get("dtypes", {})
    df_list = []
    
    for path in paths:
        print(f"Reading file: {path}")
        if path.suffix == ".csv":
            df = pd.read_csv(path, encoding=encoding, dtype=dtype_map)
        elif path.suffix in [".xls", ".xlsx"]:
            df = pd.read_excel(path, dtype=dtype_map)
        else:
            print(f"Unsupported file type: {path.suffix}")
            continue

        df_list.append(df)

    return pd.concat(df_list, ignore_index=True)
    
def extract_bad_isrcs(df, isrc_column, title_column, prefix='AAA11'):
  #filter ISRCs that do not start with the specified prefix
  bas_isrcs =  df[~df[isrc_column].str.startswith(prefix, na=False)][[isrc_column, title_column]]
  return bas_isrcs.drop_duplicates()

def add_date_column(df, report_date):
    df.loc[:, 'date'] = report_date
    return df

def clean_a_report(source, df, report_date):
    #Convert the SUM formula to an actual number by comupting the numbers ourselves
    df["Total"] = df[["Ad Revenue", "Streaming Revenue", "Copyright Revenue"]].sum(axis=1)
        
    #Replace any 'Unknown's in the view columns with null
    if source['name'] == 'a_video_report':
        df[['Total Views', 'Premium Views']] = df[['Total Views', 'Premium Views']].replace('Unknown', pd.NA)

    #Add the date column to the final data
    df = add_date_column(df, report_date)

    #Get all isrcs that do not start with our prefix
    if source['name'] == 'a_revenue_report':
        bad_isrcs = extract_bad_isrcs(df, 'ISRC', 'Title')
    else:
        bad_isrcs = None

    return df, bad_isrcs

def treat_b_revenue(report_date):
    b_revenue_sub_csv = next(pathlib.Path("source").glob("b_revenue_report_??????.csv"), None)
    b_revenue_monetize_csv = next(pathlib.Path("source").glob("b_revenue_report_v2_*.csv"), None)
    if not b_revenue_sub_csv or not b_revenue_monetize_csv:
        return None, None
    
    b_revenue_sub_report = pd.read_csv(b_revenue_sub_csv, dtype= {'request_month': 'str', 'streams_downloads': 'Int64'})
    b_revenue_monetize_report = pd.read_csv(b_revenue_monetize_csv, dtype= {'request_month': 'str', 'streams_downloads': 'Int64'}, encoding='cp932') #check encoding
    
    #drop the final row from the revenue report
    b_revenue_sub_report = b_revenue_sub_report[:-1].copy()
    b_revenue_monetize_report = b_revenue_monetize_report[:-1].copy()

    #Combine the two revenue reports
    df = pd.concat([b_revenue_monetize_report, b_revenue_sub_report], ignore_index=True)

    #Add the date column to the final data
    df = add_date_column(df, report_date)

    #Get all isrcs that do not start with our prefix
    bad_isrcs = extract_bad_isrcs(df, 'ISRC', 'title')

    return df, bad_isrcs

def run(source, report_date):
    if source['cleaner'] == 'a_data':
        # Collect all files that match the source
        paths = collect_files(source)
        if not paths:
            return None, None
        
        # Combine all files into a single dataframe
        df_initial = combine_dataframes(source, paths)

        return clean_a_report(source, df_initial, report_date)
    elif source['cleaner'] == 'b_data':
        #Need special treatment for b revenue reports as they are so helpless
        return treat_b_revenue(report_date)
    else:
        raise ValueError(f"Unknown cleaner: {source['cleaner']}")