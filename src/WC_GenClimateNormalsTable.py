#!/usr/bin/python3
import json
import argparse
import pymysql
import os

# Load configuration from JSON file
def load_config(config_path):
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"The configuration file at {config_path} does not exist.")
    
    with open(config_path, 'r') as file:
        return json.load(file)
    
# Connect to MySQL database using pymysql
def connect_to_db(config):
    return pymysql.connect(
        host=config["host"],
        user=config["user"],
        password=config["password"],
        database=config["database"],
        cursorclass=pymysql.cursors.DictCursor
    )

# Create the destination table if it doesn't exist
def create_normals_table(cursor, table_name, force_recreate):
    if force_recreate:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        DayOfYear VARCHAR(5) NOT NULL PRIMARY KEY,
        AvgTempAvg DECIMAL(3,1) NULL,
        AvgTempHigh DECIMAL(3,1) NULL,
        AvgTempLow DECIMAL(3,1) NULL,
        MinTempAvg DECIMAL(3,1) NULL,
        MaxTempAvg DECIMAL(3,1) NULL,
        MinTempHigh DECIMAL(3,1) NULL,
        MaxTempHigh DECIMAL(3,1) NULL,
        MinTempLow DECIMAL(3,1) NULL,
        MaxTempLow DECIMAL(3,1) NULL,
        AvgDewPointAvg DECIMAL(3,1) NULL,
        AvgDewPointHigh DECIMAL(3,1) NULL,
        AvgDewPointLow DECIMAL(3,1) NULL,
        MinDewPointAvg DECIMAL(3,1) NULL,
        MaxDewPointAvg DECIMAL(3,1) NULL,
        MinDewPointHigh DECIMAL(3,1) NULL,
        MaxDewPointHigh DECIMAL(3,1) NULL,
        MinDewPointLow DECIMAL(3,1) NULL,
        MaxDewPointLow DECIMAL(3,1) NULL,
        AvgHumidityAvg INT(3) NULL,
        AvgHumidityHigh INT(3) NULL,
        AvgHumidityLow INT(3) NULL,
        MinHumidityAvg INT(3) NULL,
        MaxHumidityAvg INT(3) NULL,
        MinHumidityHigh INT(3) NULL,
        MaxHumidityHigh INT(3) NULL,
        MinHumidityLow INT(3) NULL,
        MaxHumidityLow INT(3) NULL,
        AvgPressureAvg DECIMAL(5,1) NULL,
        AvgPressureHigh DECIMAL(5,1) NULL,
        AvgPressureLow DECIMAL(5,1) NULL,
        MinPressureAvg DECIMAL(5,1) NULL,
        MaxPressureAvg DECIMAL(5,1) NULL,
        MinPressureHigh DECIMAL(5,1) NULL,
        MaxPressureHigh DECIMAL(5,1) NULL,
        MinPressureLow DECIMAL(5,1) NULL,
        MaxPressureLow DECIMAL(5,1) NULL,
        AvgWindSpeedMax DECIMAL(4,1) NULL,
        MaxWindSpeedMax DECIMAL(4,1) NULL,
        AvgGustSpeedMax DECIMAL(4,1) NULL,
        MaxGustSpeedMax DECIMAL(4,1) NULL,
        AvgPrecipitationSum DECIMAL(4,1) NULL,
        MinPrecipitationSum DECIMAL(4,1) NULL,
        MaxPrecipitationSum DECIMAL(4,1) NULL,
        AvgSolarRadiation DECIMAL(5,1) NULL,
        MaxSolarRadiation DECIMAL(5,1) NULL,
        AvgUVIndex DECIMAL(3,1) NULL,
        MaxUVIndex DECIMAL(3,1) NULL,
        AvgWindChill DECIMAL(3,1) NULL,
        MinWindChill DECIMAL(3,1) NULL,
        MaxWindChill DECIMAL(3,1) NULL,
        AvgHeatIndex DECIMAL(3,1) NULL,
        MinHeatIndex DECIMAL(3,1) NULL,
        MaxHeatIndex DECIMAL(3,1) NULL
    )'''
    cursor.execute(create_table_query)

# Compute climate normals based on the cross-reference configuration only
def compute_normals(cursor, source_table, crossref, period):
    """
    Generates a SQL query to compute climate normals using only the mappings 
    from the provided cross-reference configuration.

    :param cursor: Database cursor object for executing queries.
    :param source_table: Name of the source table in the database.
    :param crossref: Dictionary mapping destination fields to their corresponding SQL expressions.
    :param period: Dictionary containing the start and end years for the computation.
    :return: List of rows containing computed climate normals.
    """

    # Construct the SQL select clause based on the crossref configuration
    select_fields = ", ".join([f"{expr} AS {field}" for field, expr in crossref.items()])

    query = f'''
    SELECT DATE_FORMAT(Date, '%m-%d') AS DayOfYear, {select_fields}
    FROM {source_table}
    WHERE YEAR(Date) BETWEEN {period['start_year']} AND {period['end_year']}
    GROUP BY DayOfYear
    '''
    
    cursor.execute(query)
    return cursor.fetchall()


# Insert computed data into the destination table
def insert_normals(cursor, table_name, data):
    if not data:
        return
    placeholders = ", ".join(["%s"] * len(data[0]))
    query = f"INSERT INTO {table_name} VALUES ({placeholders})"
    cursor.executemany(query, [tuple(row.values()) for row in data])

# Main function
def main():
    parser = argparse.ArgumentParser(description="Generate climate normals table")
    parser.add_argument("--config", required=True, help="Path to the JSON configuration file")
    parser.add_argument("--force", action="store_true", help="Force recreation of the table if it exists")
    args = parser.parse_args()
    
    config = load_config(args.config)
    force_recreate = args.force
    
    source_db = config["source_db"]
    destination_db = config["destination_db"]
    period = config["period"]
    crossref = config["crossref"]
    
    table_name = destination_db["table"].replace("<year_start>", str(period["start_year"]))\
                                  .replace("<year_end>", str(period["end_year"]))
    
    src_conn = connect_to_db(source_db)
    dest_conn = connect_to_db(destination_db)
    
    try:
        with src_conn.cursor() as src_cursor, dest_conn.cursor() as dest_cursor:
            create_normals_table(dest_cursor, table_name, force_recreate)
            data = compute_normals(src_cursor, source_db["table"], crossref, period)
            if data:
                insert_normals(dest_cursor, table_name, data)
            dest_conn.commit()
    finally:
        src_conn.close()
        dest_conn.close()
    
    print(f"Climate normals generated and stored in {table_name}")

if __name__ == "__main__":
    main()

