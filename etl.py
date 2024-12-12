import duckdb

BEGINNING_OF_FILE_PATH = "/Users/ryanwaldorf/dev/random_stuff/mongo_db_streaming/assets"
DB_PATH = f"{BEGINNING_OF_FILE_PATH}/nfl.duckdb"
PBP_START_YEAR = 2009
PBP_END_YEAR = 2023

play_by_play_bronze = "play_by_play_bronze"
play_by_play_silver = "play_by_play_silver"
player_role_mapping_csv = "player_role_mapping.csv"

# data set source:
# https://github.com/nflverse/nflverse-data/releases/tag/pbp

conn = duckdb.connect(DB_PATH)

def create_bronze_objects():

    # create role mapping table
    conn.execute(f"""CREATE OR REPLACE TABLE role_mapping AS SELECT * FROM read_csv_auto("{BEGINNING_OF_FILE_PATH}/{player_role_mapping_csv}");""")

    # create bronze table

    working_year = PBP_START_YEAR
    while working_year <= PBP_END_YEAR:
        file_path = BEGINNING_OF_FILE_PATH + "/play_by_play_" + str(working_year) + ".parquet"
        if working_year == PBP_START_YEAR:
            statement_beginning = f"CREATE OR REPLACE TABLE {play_by_play_bronze} AS "
        else:
            statement_beginning = f"INSERT INTO {play_by_play_bronze} "

        full_statement = statement_beginning + f"SELECT * FROM \"{file_path}\""
        conn.execute(full_statement)
        print(f"Finished loading play_by_play_{working_year} into bronze")
        working_year += 1
    # conn.execute(f"""CREATE OR REPLACE TABLE play_by_play_bronze as 
    # (
    # SELECT * FROM "{BEGINNING_OF_FILE_PATH}/play_by_play_2021.parquet"
    # -- UNION
    # -- SELECT * FROM "{BEGINNING_OF_FILE_PATH}/play_by_play_2022.parquet"
    # -- UNION
    # -- SELECT * FROM "{BEGINNING_OF_FILE_PATH}/play_by_play_2023.parquet"
    # )""")

def create_silver_objects():
    non_player_columns = conn.execute(f"""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_name = '{play_by_play_bronze}' and (column_name not ilike '%player%' AND column_name not ilike '%team');
        """
    ).fetchall()
    formatted_non_player_columns = format_columns(non_player_columns)

    player_columns = conn.execute(f"""SELECT revised_name, 'varchar' data_type FROM role_mapping GROUP BY 1, 2;""").fetchall()
    formatted_player_columns = format_columns(player_columns)

    formatted_silver_columns = formatted_non_player_columns + ", " + formatted_player_columns

    # create silver table
    conn.execute(f"""
        CREATE OR REPLACE TABLE {play_by_play_silver} ({formatted_silver_columns})
    """)

    role_mapping_array = conn.execute("SELECT * FROM role_mapping").fetchall()
    # print(formatted_silver_columns)
    for [old_col, new_col] in role_mapping_array:
        if "player_id" in new_col or new_col.endswith("_team"):
            continue

        old_name_col = old_col
        new_name_col = new_col
        old_id_col = old_name_col.split("player_name")[0] + "player_id"
        new_id_col = new_name_col.split("player_name")[0] + "player_id"

        relevant_columns_for_insert = ", ".join(list(map(lambda col: f"\"{col[0]}\"", non_player_columns))) + f", {new_name_col}, {new_id_col}"
        relevant_columns_for_select = ", ".join(list(map(lambda col: f"\"{col[0]}\"", non_player_columns))) + f", {old_name_col} {new_name_col}, {old_id_col} {new_id_col}"
        query = f"""INSERT INTO {play_by_play_silver} ({relevant_columns_for_insert})
        SELECT {relevant_columns_for_select} FROM {play_by_play_bronze} WHERE {old_col} IS NOT NULL;"""
        conn.execute(query)
        print(f"FINISHED INSERTING {old_name_col} AS {new_name_col} and {old_id_col} AS {new_id_col}")

def format_columns(array_of_columns):
    return ", ".join(list(map(lambda x: f"\"{x[0]}\" {x[1]}", array_of_columns)))

create_bronze_objects()
create_silver_objects()

