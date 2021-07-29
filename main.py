###############################################################################
# @@ Reservations Overlap Tracker Utility
# @@ version: v4.5
###############################################################################

# Import the Libraries:-
import pandas as pd
from sqlalchemy import create_engine
from decouple import config
import multiprocessing
import time

# Decode the Environment variables: DB Connection Strings:-
### Staging ###
MYSQL_USER_TIA = config('MYSQL_USER_TIA')
PASSWORD_STAGE = config('MYSQL_PASSWORD_TIA')
HOST_STAGE = config('HOST_TIA')

### Sandbox/ Stage3 ###
MYSQL_USER_TRACK_STAGE = config('MYSQL_USER_TRACK_STAGE')
PASSWORD_SANDBOX = config('MYSQL_PASSWORD_TRACK_STAGE')
HOST_SANDBOX = config('HOST_TRACK_STAGE')

### Prod ###
MYSQL_USER_TRACK_PROD = config('MYSQL_USER_TRACK_PROD')
PASSWORD_PROD = config('MYSQL_PASSWORD_TRACK_PROD')
HOST_PROD = config('HOST_TRACK_PROD')

### LOCAL ###
LOCAL_USER = config('LOCAL_USER')
LOCAL_PASSWORD = config('LOCAL_PASSWORD')
LOCAL_HOST = config('LOCAL_HOST')

# DB Engine Params:-
local = create_engine(f"mysql+pymysql://{LOCAL_USER}:{LOCAL_PASSWORD}@{LOCAL_HOST}/meredithdb")
stage = create_engine(f"mysql+pymysql://{MYSQL_USER_TIA}:{PASSWORD_STAGE}@{HOST_STAGE}/a3d3ac7f-0a70-45e3-9bf0-c916400c9923")


def processor_configurator(source_count):
    global chunks, blocks
    if source_count >= 20000:
        blocks = 20
        chunks = int(source_count / blocks) + int(source_count % blocks)
    elif 500 <= source_count < 20000:
        blocks = 20
        chunks = int(source_count / blocks) + int(source_count % blocks)
    elif 20 <= source_count < 500:
        blocks = 10
        chunks = int(source_count / blocks) + int(source_count % blocks)
    elif 0 <= source_count < 20:
        blocks = 1
        chunks = int(source_count / blocks) + int(source_count % blocks)
    return chunks, blocks


def chunker(units_list, chunk_size, blocks):
    j = 1
    splits = []
    for j in range(blocks + 1):
        splits.append(units_list[chunk_size * (j - 1):chunk_size * j])
    return splits


def overlap_multi_processor(j, chunk_size, blocks, units_list):
    # TRACK the workers:-
    print("# Worker-Tracker:", j)

    # Create Chunks of Units List:-
    splits = chunker(units_list, chunk_size, blocks)
    units_filt_list = splits[j]

    ## Initialize the list vars:-
    worker_list = []
    source_index = []
    match_index = []
    source_software_platform = []
    o_software_platform = []
    source_cabin_id = []
    o_cabin_id = []
    source_folio_list = []
    o_folio_list = []
    source_arrival = []
    source_departure = []
    o_arrival = []
    o_departure = []
    source_first_name = []
    source_last_name = []
    source_email = []
    o_first_name = []
    o_last_name = []
    o_email = []
    overlap_arrival_flag_list = []
    overlap_departure_flag_list = []

    for u in units_filt_list:

        ## Read filtered source dataset - Unit-wise:-

        sql = f"""
                SELECT * FROM edi_src_hist_v12_escapia_reservation_report_ctrl_test f
                WHERE f.`Track Unit_ID` = {u};
        """
        df = pd.read_sql(sql, con=local)

        ## Master Overlap finder loop:-

        for index, row in df.iterrows():
            for i in range(0, len(df)):
                if (df['Track Unit_ID'][index] == df['Track Unit_ID'][i]) and (index != i):

                    # Check for Arrival Date & Dept Overlaps:-
                    if ((df['Arrival_Data_Mod'][index] <= df['Arrival_Data_Mod'][i])
                          and (df['True_Departure_Date_Fix_Mod'][index] >= df['True_Departure_Date_Fix_Mod'][i])):

                        print(
                            f"~ Arrival Date & Departure Date Overlap: True, worker:{j}, src_index:{index},overlap_index:{i}")

                        worker_list.append(j)
                        source_index.append(index)
                        match_index.append(i)

                        source_software_platform.append(df['Software Platform'][index])
                        o_software_platform.append(df['Software Platform'][i])
                        source_cabin_id.append(df['Track Unit_ID'][index])
                        o_cabin_id.append(df['Track Unit_ID'][i])

                        source_folio_list.append(df['Booking_Number'][index])
                        o_folio_list.append(df['Booking_Number'][i])

                        source_arrival.append(df['Arrival_Data_Mod'][index])
                        source_departure.append(df['True_Departure_Date_Fix_Mod'][index])
                        o_arrival.append(df['Arrival_Data_Mod'][i])
                        o_departure.append(df['True_Departure_Date_Fix_Mod'][i])

                        source_first_name.append(df['First Name'][index])
                        source_last_name.append(df['Last Name'][index])
                        source_email.append(df['Email'][index])
                        o_first_name.append(df['First Name'][i])
                        o_last_name.append(df['Last Name'][i])
                        o_email.append(df['Email'][i])

                        overlap_arrival_flag_list.append(True)
                        overlap_departure_flag_list.append(True)

                    # Arrival No Overlap and Departure Date Overlap:-
                    if ((df['Arrival_Data_Mod'][index] > df['Arrival_Data_Mod'][i])
                            and (df['True_Departure_Date_Fix_Mod'][index] > df['True_Departure_Date_Fix_Mod'][i])
                                and (df['Arrival_Data_Mod'][index] < df['True_Departure_Date_Fix_Mod'][i])
                        ):

                        print(
                            f"Arrival No Overlap and Departure Date Overlap, worker:{j}, src_index:{index},overlap_index:{i}")

                        worker_list.append(j)
                        source_index.append(index)
                        match_index.append(i)

                        source_software_platform.append(df['Software Platform'][index])
                        o_software_platform.append(df['Software Platform'][i])
                        source_cabin_id.append(df['Track Unit_ID'][index])
                        o_cabin_id.append(df['Track Unit_ID'][i])

                        source_folio_list.append(df['Booking_Number'][index])
                        o_folio_list.append(df['Booking_Number'][i])

                        source_arrival.append(df['Arrival_Data_Mod'][index])
                        source_departure.append(df['True_Departure_Date_Fix_Mod'][index])
                        o_arrival.append(df['Arrival_Data_Mod'][i])
                        o_departure.append(df['True_Departure_Date_Fix_Mod'][i])

                        source_first_name.append(df['First Name'][index])
                        source_last_name.append(df['Last Name'][index])
                        source_email.append(df['Email'][index])
                        o_first_name.append(df['First Name'][i])
                        o_last_name.append(df['Last Name'][i])
                        o_email.append(df['Email'][i])

                        overlap_arrival_flag_list.append(False)
                        overlap_departure_flag_list.append(True)

                    # Arrival Date Overlap and Departure No Overlap:-
                    if (
                        (df['Arrival_Data_Mod'][index] < df['Arrival_Data_Mod'][i])
                            and (df['True_Departure_Date_Fix_Mod'][index] < df['True_Departure_Date_Fix_Mod'][i])
                                and (df['True_Departure_Date_Fix_Mod'][index] > df['Arrival_Data_Mod'][i])
                        ):

                        print(
                            f"Arrival Date Overlap and Departure No Overlap, worker:{j}, src_index:{index},overlap_index:{i}")

                        worker_list.append(j)
                        source_index.append(index)
                        match_index.append(i)

                        source_software_platform.append(df['Software Platform'][index])
                        o_software_platform.append(df['Software Platform'][i])
                        source_cabin_id.append(df['Track Unit_ID'][index])
                        o_cabin_id.append(df['Track Unit_ID'][i])

                        source_folio_list.append(df['Booking_Number'][index])
                        o_folio_list.append(df['Booking_Number'][i])

                        source_arrival.append(df['Arrival_Data_Mod'][index])
                        source_departure.append(df['True_Departure_Date_Fix_Mod'][index])
                        o_arrival.append(df['Arrival_Data_Mod'][i])
                        o_departure.append(df['True_Departure_Date_Fix_Mod'][i])

                        source_first_name.append(df['First Name'][index])
                        source_last_name.append(df['Last Name'][index])
                        source_email.append(df['Email'][index])
                        o_first_name.append(df['First Name'][i])
                        o_last_name.append(df['Last Name'][i])
                        o_email.append(df['Email'][i])

                        overlap_arrival_flag_list.append(True)
                        overlap_departure_flag_list.append(False)

                    # No Overlaps:-
                    if (((df['Arrival_Data_Mod'][index] < df['Arrival_Data_Mod'][i]) and (df['True_Departure_Date_Fix_Mod'][index] < df['True_Departure_Date_Fix_Mod'][i]))
                    or ((df['Arrival_Data_Mod'][index] > df['Arrival_Data_Mod'][i]) and (df['True_Departure_Date_Fix_Mod'][index] > df['True_Departure_Date_Fix_Mod'][i]))):

                        print(f"No Overlaps, worker:{j}, src_index:{index},overlap_index:{i}")

                        worker_list.append(j)
                        source_index.append(index)
                        match_index.append(i)

                        source_software_platform.append(df['Software Platform'][index])
                        o_software_platform.append(df['Software Platform'][i])
                        source_cabin_id.append(df['Track Unit_ID'][index])
                        o_cabin_id.append(df['Track Unit_ID'][i])

                        source_folio_list.append(df['Booking_Number'][index])
                        o_folio_list.append(df['Booking_Number'][i])

                        source_arrival.append(df['Arrival_Data_Mod'][index])
                        source_departure.append(df['True_Departure_Date_Fix_Mod'][index])
                        o_arrival.append(df['Arrival_Data_Mod'][i])
                        o_departure.append(df['True_Departure_Date_Fix_Mod'][i])

                        source_first_name.append(df['First Name'][index])
                        source_last_name.append(df['Last Name'][index])
                        source_email.append(df['Email'][index])
                        o_first_name.append(df['First Name'][i])
                        o_last_name.append(df['Last Name'][i])
                        o_email.append(df['Email'][i])

                        overlap_arrival_flag_list.append(False)
                        overlap_departure_flag_list.append(False)

    # Prep the temp df:-
    df_temp = pd.DataFrame(
        {"worker": worker_list,
         "source_index": source_index,
         "match_index": match_index,
         "source_software_platform": source_software_platform,
         "o_software_platform": o_software_platform,
         "source_cabin_id": source_cabin_id,
         "o_cabin_id": o_cabin_id,
         "source_folio_list": source_folio_list,
         "o_folio_list": o_folio_list,
         "source_arrival": source_arrival,
         "source_departure": source_departure,
         "o_arrival": o_arrival,
         "o_departure": o_departure,
         "source_first_name": source_first_name,
         "source_last_name": source_last_name,
         "source_email": source_email,
         "o_first_name": o_first_name,
         "o_last_name": o_last_name,
         "o_email": o_email,
         "overlap_arrival_flag_list": overlap_arrival_flag_list,
         "overlap_departure_flag_list": overlap_departure_flag_list
         })

    df_temp.to_sql("tia_hist_meredith_overlap_temp_ctrl", con=local, index=False, if_exists="append")
    print("~ Appended the temp Output df ---> Local")


def start_multiprocessor(chunk_size, blocks, units_list):
    # Creating Multi-processes for the splits and multiple rest api calls-
    print('Multi-Processor Start')
    processes = []
    j = 1
    for j in range(blocks + 1):
        s = multiprocessing.Process(target=overlap_multi_processor, args=(j, chunk_size, blocks, units_list))
        print(f'Process-Initialized: {j}')
        processes.append(s)

    for process in processes:
        process.start()
        time.sleep(10)

    for process in processes:
        process.join()


def remove_swapped_reservations():
    # Remove Swapped Reservations:-
    sql = """
            SELECT *
            FROM tia_hist_meredith_overlap_temp_ctrl t
            WHERE t.source_folio_list <= t.o_folio_list
            UNION ALL
            SELECT *
            FROM tia_hist_meredith_overlap_temp_ctrl t
            WHERE t.source_folio_list > t.o_folio_list
            AND NOT EXISTS (
            SELECT 1
            FROM tia_hist_meredith_overlap_temp_ctrl t2
            WHERE t.source_folio_list = t2.o_folio_list
            AND t2.source_folio_list = t.o_folio_list);

            """
    df = pd.read_sql(sql, con=local)

    df.to_sql("tia_hist_meredith_overlap_out_ctrl", con=local, index=False, if_exists='replace')


def backup_to_stage():

    ## Backup temp and df_out ---> stage:-
    sql_temp = """
                    SELECT * FROM tia_hist_meredith_overlap_temp_ctrl t;

                """
    sql_out = """
                    SELECT * FROM tia_hist_meredith_overlap_out_ctrl o;
                """

    df_temp = pd.read_sql(sql_temp, con=local)
    df_out = pd.read_sql(sql_out, con=local)

    df_temp.to_sql("tia_hist_meredith_overlap_temp_ctrl", con=stage, index=False, if_exists="replace")
    df_out.to_sql("tia_hist_meredith_overlap_out_ctrl", con=stage, index=False, if_exists="replace")


if __name__ == '__main__':
    ## Main Loop:-
    print("\n#########################################################")
    print("##### Reservation Overlap Detector Utility Tool #########")
    print("#########################################################")

    ## Read source dataset:-

    sql = """
            SELECT * FROM edi_src_hist_v12_escapia_reservation_report_ctrl_test f;
        """
    df_src = pd.read_sql(sql, con=local)  ## 99305

    # Extract List of Unique Units:-
    units_list = df_src['Track Unit_ID'].unique().tolist()

    print("@ Extracted unique list of units")

    source_count = len(units_list)
    print(f"@ Read Source Units Data: Count {source_count}")

    # Final Table Initialize:-
    df_temp = pd.DataFrame(
        {"worker": pd.Series([], dtype='int'),
         "source_index": pd.Series([], dtype='int'),
         "match_index": pd.Series([], dtype='int'),
         "source_software_platform": pd.Series([], dtype='str'),
         "o_software_platform": pd.Series([], dtype='str'),
         "source_cabin_id": pd.Series([], dtype='int'),
         "o_cabin_id": pd.Series([], dtype='int'),
         "source_folio_list": pd.Series([], dtype='str'),
         "o_folio_list": pd.Series([], dtype='str'),
         "source_arrival": pd.Series([], dtype='datetime64[ns]'),
         "source_departure": pd.Series([], dtype='datetime64[ns]'),
         "o_arrival": pd.Series([], dtype='datetime64[ns]'),
         "o_departure": pd.Series([], dtype='datetime64[ns]'),
         "source_first_name": pd.Series([], dtype='str'),
         "source_last_name": pd.Series([], dtype='str'),
         "source_email": pd.Series([], dtype='str'),
         "o_first_name": pd.Series([], dtype='str'),
         "o_last_name": pd.Series([], dtype='str'),
         "o_email": pd.Series([], dtype='str'),
         "overlap_arrival_flag_list": pd.Series([], dtype='bool'),
         "overlap_departure_flag_list": pd.Series([], dtype='bool')
         })

    df_temp.to_sql("tia_hist_meredith_overlap_temp_ctrl", con=local, index=False, if_exists="replace")
    df_temp.to_sql("tia_hist_meredith_overlap_out_ctrl", con=local, index=False, if_exists="replace")

    print("@ Temp table: tia_hist_meredith_overlap_temp ~ Initialized")

    # Initialize the no of processes to run in parallel and df chunk size
    chunk_size, blocks = processor_configurator(source_count)

    print(f"@ Extracted Chunk size and worker count: Chunk_size: {chunk_size}, blocks: {blocks}")

    # Creating Multi-processes for the splits and multiple rest api calls-
    start_multiprocessor(chunk_size, blocks, units_list)

    # Remove Swapped Reservations:-
    remove_swapped_reservations()

    # Backup data --> Stage:-
    backup_to_stage()
