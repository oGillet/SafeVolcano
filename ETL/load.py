import pandas as pd
import glob
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
from collect import save_to_csv
import os
from pathlib import Path
from geoalchemy2 import Geometry, WKTElement
import geopandas as gpd

# Load environment variables
load_dotenv()

def get_db_connection():
    """Create and return a PostgreSQL database connection and SQLAlchemy engine"""
    db_url = f"postgresql://{os.getenv('DB_USER', 'oliviergillet')}:{os.getenv('DB_PASSWORD', 'volcanic')}@" \
             f"{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/" \
             f"{os.getenv('DB_NAME', 'volcanic_etl')}"

    engine = create_engine(db_url)
    conn = engine.connect()
    return conn, engine

def filter_volcanoes_by_names(con, engine, csv_file):
    """Filter volcanoes_db records where volcn_nm matches names in the CSV"""
    # Read CSV file
    df = pd.read_csv(csv_file)

    # Get unique names from the CSV
    names = df['Name'].dropna().unique().tolist()

    if not names:
        print("No valid names found in the CSV file")
        return pd.DataFrame()

    try:

        placeholders = ','.join(['%s'] * len(names))
        query = f"""
                    SELECT *
                    FROM volcanoes_db
                    WHERE "Volcano_Name" IN ({placeholders})
                """

        with engine.connect() as conn:
            params = tuple(names)
            result_df = pd.read_sql(query, conn, params=params)

        return result_df, df.shape[0]

    except Exception as e:
        print(f"Error querying database: {str(e)}")
        return pd.DataFrame()

def load_data_to_postgres(conn, engine, dataset, table_name):
    """Load data from CSV file to PostgreSQL table using pandas"""

    try:
        # Load data using pandas with SQLAlchemy engine
        dataset.to_sql(
            table_name,
            engine,
            if_exists='replace',
            index=False,
            method='multi'
        )
        print(f"Successfully loaded {dataset} into table {table_name}")
    except Exception as e:
        print(f"Error loading {dataset}: {str(e)}")
        raise

def table_exists(engine, table_name):
    """Check if a table exists using SQLAlchemy"""
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()

def load_geodataframe_to_postgis(
    engine,
    gdf: gpd.GeoDataFrame,
    table_name: str,
    schema: str = "public",
    if_exists: str = "replace",
    geom_col: str = "geometry",
    srid: int = 4326
):
    """
    Load a GeoDataFrame into PostgreSQL/PostGIS.
    """
    try:
        # Convert geometry to WKT (Well-Known Text) for GeoAlchemy
        gdf = gdf.copy()
        gdf[geom_col] = gdf[geom_col].apply(
            lambda geom: WKTElement(geom.wkt, srid=srid) if geom else None
        )

        # Write to PostgreSQL
        gdf.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            dtype={geom_col: Geometry(geometry_type='GEOMETRY', srid=srid)},
            method="multi"  # Faster for bulk inserts
        )

        print(f"✅ Successfully loaded GeoDataFrame into {schema}.{table_name}")

    except Exception as e:
        print(f"❌ Error loading GeoDataFrame: {str(e)}")
        raise

if __name__ == "__main__":

    current_dir = Path.cwd()

    # Process each CSV file
    conn, engine = get_db_connection()

    volcanoes_db = pd.read_csv(str(current_dir / 'data/world_actives_volcanoes_db.csv'))
    if volcanoes_db is not None:
        load_data_to_postgres(conn, engine, volcanoes_db, "volcanoes_db")

    historical_eruptions_db = pd.read_csv(str(current_dir / 'data/historical_eruptions_db.csv'))
    if historical_eruptions_db is not None:
        load_data_to_postgres(conn, engine, historical_eruptions_db, "historical_eruptions_db")

    earth_quake_db = pd.read_csv(str(current_dir / 'data/earth_quake_db.csv'))
    if earth_quake_db is not None:
        load_data_to_postgres(conn, engine, earth_quake_db, "earth_quake_db")

    alerts_volcanoes_latest = pd.read_csv(str(current_dir / 'data/alerts_volcanoes_latest.csv'))
    if alerts_volcanoes_latest is not None:
        load_data_to_postgres(conn, engine, alerts_volcanoes_latest, "alerts_volcanoes_latest")

    # Define patterns relative to current_dir
    csv_patterns = [
        str(current_dir / 'data/erupting*.csv'),
        str(current_dir / 'data/unrest*.csv'),
    ]

    csv_files = []
    for pattern in csv_patterns:
        csv_files.extend(glob.glob(pattern))

    if not csv_files:
        print("No CSV files found matching the patterns")
        exit(1)

    try:
        for csv_file in csv_files:
            print(csv_file)
            filtered_data, expected_data = filter_volcanoes_by_names(conn, engine, csv_file)

            if not filtered_data.empty:

                print(f"Found {len(filtered_data)} matching records ({expected_data} expected):")
                print(filtered_data)
                table_name = os.path.splitext(os.path.basename(csv_file))[0]
                load_data_to_postgres(conn, engine, filtered_data, table_name)

                df_informations = pd.read_csv('ETL/app/data/information_etl.csv')
                new_row = {'information': 'matching', 'value': len(filtered_data)}
                df_informations = pd.concat([df_informations, pd.DataFrame([new_row])], ignore_index=True)
                new_row = {'information': 'expected', 'value': expected_data}
                df_informations = pd.concat([df_informations, pd.DataFrame([new_row])], ignore_index=True)
                save_to_csv(df_informations, f'information_etl.csv', "informations", to_app=True)

            else:
                print("No matching records found.")

        if table_exists(engine, "MOESM1"):
            print("Table MOESM1 exists!")
        else:
            print("Table does not exist.")
            df = pd.read_excel(str(current_dir / 'data/13617_2017_67_MOESM1_ESM.xlsx'))
            load_data_to_postgres(conn, engine, df, "MOESM1")

    finally:
        conn.close()
        engine.dispose()







