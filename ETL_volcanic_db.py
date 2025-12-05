import datetime
from datetime import datetime
from datetime import timedelta

import pendulum
import pandas as pd
import geopandas as gpd
from bs4 import BeautifulSoup
import requests
import os
import regex
from owslib.wfs import WebFeatureService
from psycopg2 import sql
import warnings
import osmnx as ox
import networkx as nx
from shapely import wkb
import pickle
import subprocess
from typing import Optional, Tuple

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable


@dag(
    dag_id="process_smithsonian",
    start_date=pendulum.now("UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=180),
)
def process_data_smithsonian():

    @task
    def extract_data_smithsonian():

        def scrape_volcanic_db():
            """
            Scrapes volcano eruption and unrest data for a given date.

            Args:
                date_str (str): Date in 'YYYY-MM-DD' format (e.g., '2025-09-15').

            Returns:
                tuple: (DataFrame of erupting volcanoes, DataFrame of unrest volcanoes)
            """
            # Build the target URL
            date_obj = datetime.now()
            date_obj = date_obj - timedelta(days=1)
            #date_str = "2025-11-24"
            date_str = date_obj.strftime("%Y-%m-%d")
            url = f"https://volcano.si.edu/reports_daily.cfm?activitydate={date_str}"

            # Headers to mimic a browser and avoid blocking
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            try:
                # Fetch the webpage
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise HTTP errors if any
                print(f"Accessed URL: {response.url}")

                soup = BeautifulSoup(response.text, 'html.parser')

                # Inner function to extract a specific section
                def extract_section(section_title):
                    section = soup.find('div', class_='SectionHeader-Variable',
                                       string=lambda text: section_title in str(text))
                    if not section:
                        print(f"⚠️ Section '{section_title}' not found.")
                        return None

                    table = section.find_next('table')
                    if not table:
                        print(f"⚠️ No table found for '{section_title}'.")
                        return None

                    headers = []
                    thead = table.find('thead')
                    if thead:
                        headers = [th.get_text(strip=True) for th in thead.find_all('th')]

                    data = []
                    for row in table.find('tbody').find_all('tr'):
                        cols = row.find_all('td')
                        row_data = []
                        for col in cols:
                            text = col.get_text(strip=True)
                            link = col.find('a')
                            if link:
                                text = link.get_text(strip=True)
                            row_data.append(text)
                        data.append(row_data)

                    return pd.DataFrame(data, columns=headers)

                # Dynamic section titles based on the input date
                formatted_date = datetime.strptime(date_str, '%Y-%m-%-d').strftime('%d %B %Y')
                eruption_title = f"List of Volcanoes with Eruptive Activity on {formatted_date}"
                unrest_title = f"List of Volcanoes with Unrest on {formatted_date}"

                erupting_df = extract_section(eruption_title)
                unrest_df = extract_section(unrest_title)

                return erupting_df, unrest_df

            except requests.exceptions.RequestException as e:
                print(f"❌ Error fetching the webpage: {e}")
                return None, None
            except Exception as e:
                print(f"❌ An unexpected error occurred: {e}")
                return None, None

        def scrape_volcano_reports_alerts():
            """
            Scrapes the 'Volcano Reports' tab for a date and returns
            a DataFrame with volcano name, Observatory Alert Level, and Aviation Alert Level.

            Args:
                date_str (str): 'YYYY-MM-DD' (e.g., '2025-09-14').

            Returns:
                pd.DataFrame with columns:
                ['Volcano', 'Observatory Alert Level', 'Aviation Alert Level']
            """
            # Build the target URL
            date_obj = datetime.now()
            date_obj = date_obj - timedelta(days=1)
            #date_str = "2025-11-24"
            date_str = date_obj.strftime("%Y-%m-%d")
            url = f"https://volcano.si.edu/reports_daily.cfm?activitydate={date_str}"

            # Headers to mimic a browser and avoid blocking
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            try:
                # Fetch the webpage
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise HTTP errors if any
                print(f"Accessed URL: {response.url}")

                soup = BeautifulSoup(response.text, 'html.parser')

                def extract_section(section_title):
                    section = soup.find('div', class_='SectionHeader-Variable',
                                        string=lambda text: section_title in str(text))
                    if not section:
                        print(f"⚠️ Section '{section_title}' not found.")
                        return None

                    table = section.find_all_next('table')
                    if not table:
                        print(f"⚠️ No table found for '{section_title}'.")
                        return None

                    rows = []

                    for table in soup.select('table.DivTable[role="presentation"]'):
                        head_tr = table.select_one("tr[id^='vn_']")
                        if not head_tr:
                            continue

                        # From header: name and header status
                        h5 = head_tr.find("h5")
                        title = h5.get_text(" ", strip=True) if h5 else ""
                        parts = [p.strip() for p in title.split("|", 1)]
                        name = parts[0] if parts else None

                        # Observatory/Aviation levels from the right <td>
                        detail_tr = head_tr.find_next_sibling("tr")
                        right_td = detail_tr.find_all("td")[1] if detail_tr and len(detail_tr.find_all("td")) > 1 else None
                        obs_level = avn_level = None
                        if right_td:
                            text = regex.sub(r"\s+", " ", right_td.get_text(" ", strip=True)).strip()
                            m_obs = regex.search(r"Observatory Alert Level:\s*\"?([^\"(]+)\"?", text, regex.I)
                            m_avn = regex.search(r"Aviation Alert Level:\s*\"?([^\"(]+)\"?:", text, regex.I)
                            obs_level = m_obs.group(1).strip() if m_obs else None
                            avn_level = m_avn.group(1).strip() if m_avn else None
                            if not avn_level:
                                m_avn2 = regex.search(r"aviation alert level.*?(?:to|at)\s*\"?([A-Za-z]+)\"?", text,
                                                      regex.I)
                                avn_level = m_avn2.group(1).strip() if m_avn2 else "unavailable or not collected"
                            if obs_level and regex.search(r"unavailable|not collected", obs_level, regex.I):
                                obs_level = "unavailable or not collected"

                        rows.append({
                            "Name": name,
                            "observatory_level": obs_level,
                            "aviation_level": avn_level
                        })

                    df = pd.DataFrame(rows)
                    return df

                formatted_date = datetime.strptime(date_str, '%Y-%m-%-d').strftime('%d %B %Y')
                alert_title = f"Reports for Volcanoes with Eruptive Activity on {formatted_date}"
                alert_df = extract_section(alert_title)

                return alert_df

            except requests.exceptions.RequestException as e:
                print(f"❌ Error fetching the webpage: {e}")
                return None, None
            except Exception as e:
                print(f"❌ An unexpected error occurred: {e}")
                return None, None

        def download_wfs_points_to_csv(wfs_url, typename):
            """
            Downloads point data from WFS, extracts coordinates, and saves to CSV.

            Args:
                wfs_url (str): WFS service URL
                typename (str): Layer name to download
            """
            try:
                # Connect to WFS
                wfs = WebFeatureService(url=wfs_url, version='2.0.0')

                # Get the feature data
                response = wfs.getfeature(typename=typename, outputFormat='json')

                # Convert to GeoDataFrame
                gdf = gpd.read_file(response)

                # Extract coordinates from Point geometry
                gdf['x_coordinate'] = gdf.geometry.x
                gdf['y_coordinate'] = gdf.geometry.y

                # Convert to pandas DataFrame (without geometry)
                df = pd.DataFrame(gdf.drop(columns='geometry'))

                return df
                print(f"Successfully saved {len(df)} features to {output_csv_path}")

            except Exception as e:
                print(f"Error: {str(e)}")

        def scrape_earthquake_data():
            """
            Scrapes latest earthquake.

            Returns:
                tuple: (DataFrame of erupting volcanoes, DataFrame of unrest volcanoes)
            """

            # USGS GeoJSON feed for all earthquakes in the past day
            url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

            try:
                # Fetch the webpage
                response = requests.get(url)
                response.raise_for_status()  # Raise HTTP errors if any
                print(f"Accessed URL: {response.url}")

                data = response.json()

                # Extract relevant fields: magnitude, latitude, longitude
                records = []
                for feature in data["features"]:
                    coords = feature["geometry"]["coordinates"]  # [longitude, latitude, depth]
                    mag = feature["properties"]["mag"]
                    place = feature["properties"]["place"]
                    infos = feature["properties"]["url"]
                    x_coordinate, y_coordinate, depth = coords[0], coords[1], coords[2]
                    records.append({"magnitude": mag, "place": place, "infos": infos, "y_coordinate": y_coordinate,
                                    "x_coordinate": x_coordinate, "depth": depth})

                # Create DataFrame
                df = pd.DataFrame(records)

                return df

            except requests.exceptions.RequestException as e:
                print(f"❌ Error fetching the webpage: {e}")
                return None, None
            except Exception as e:
                print(f"❌ An unexpected error occurred: {e}")
                return None, None

        def get_data(erupting_df = None, unrest_df = None, alerts_df = None, volcanoes_db = None, eruptions_db = None, earthquakes_db = None):

            def ensure_table_exists(hook, table_name, df, truncate_if_exists=True):
                """Check if table exists, create if not."""
                conn = hook.get_conn()
                cursor = conn.cursor()

                # Check if table exists
                cursor.execute(
                    sql.SQL("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_name = %s
                        )
                    """),
                    (table_name,)
                )
                table_exists = cursor.fetchone()[0]

                if not table_exists:
                    # Generate CREATE TABLE statement from DataFrame
                    columns = []
                    for col, dtype in df.dtypes.items():
                        if dtype == 'int64':
                            col_type = 'INTEGER'
                        elif dtype == 'float64':
                            col_type = 'FLOAT'
                        else:
                            col_type = 'TEXT'  # Default for strings, dates, etc.
                        columns.append(sql.SQL("{} {}").format(
                            sql.Identifier(col),
                            sql.SQL(col_type)
                        ))

                    # Create the table
                    cursor.execute(
                        sql.SQL("CREATE TABLE {} ({})").format(
                            sql.Identifier(table_name),
                            sql.SQL(", ").join(columns)
                        )
                    )
                    conn.commit()
                    print(f"Table {table_name} created.")
                else:
                    if truncate_if_exists:
                        # Delete all rows (faster than DELETE FROM)
                        cursor.execute(
                            sql.SQL("TRUNCATE TABLE {}").format(
                                sql.Identifier(table_name)
                            )
                        )
                        conn.commit()
                        print(f"All rows in {table_name} truncated.")
                    else:
                        print(f"Table {table_name} already exists (no truncation).")

                cursor.close()

            def insert_rows(hook, table_name, df):
                """Insert DataFrame rows into PostgreSQL table."""
                conn = hook.get_conn()
                cursor = conn.cursor()

                # Generate column names and placeholders
                columns = df.columns.tolist()
                placeholders = [sql.Placeholder()] * len(columns)

                # Build INSERT query
                query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                    sql.SQL(", ").join(placeholders)
                )

                # Insert rows
                for _, row in df.iterrows():
                    cursor.execute(query, row.tolist())
                conn.commit()
                print(f"Inserted {len(df)} rows into {table_name}.")

                cursor.close()

            date_obj = datetime.now()
            date_obj = date_obj - timedelta(days=1)
            #date_str = "2025-11-24"
            date_str = date_obj.strftime("%Y-%m-%d")

            postgres_hook = PostgresHook(postgres_conn_id="volcanic_etl")

            data_paths = {
                "erupting": f'/home/gillet/Bureau/Volcanic_ETL/data/erupting_volcanoes_{date_str}.csv',
                "unrest": f'/home/gillet/Bureau/Volcanic_ETL/data/unrest_volcanoes_{date_str}.csv',
                "volcanoes_db": "/home/gillet/Bureau/Volcanic_ETL/data/world_actives_volcanoes_db.csv",
                "historical_eruptions_db": "/home/gillet/Bureau/Volcanic_ETL/data/historical_eruptions_db.csv",
                "earthquakes_db": f'/home/gillet/Bureau/Volcanic_ETL/data/earthquakes_{date_str}.csv',
                "alerts": f'/home/gillet/Bureau/Volcanic_ETL/data/alerts_volcanoes_{date_str}.csv'
            }

            for path in data_paths.values():
                os.makedirs(os.path.dirname(path), exist_ok=True)

            if volcanoes_db is not None and not volcanoes_db.empty:
                volcanoes_db.to_csv(data_paths["volcanoes_db"], index=False)
                ensure_table_exists(postgres_hook, 'volcanoes_db', volcanoes_db)
                insert_rows(postgres_hook, 'volcanoes_db', volcanoes_db)
                print(f"✅ volcanoes_db saved to {data_paths["volcanoes_db"]} ({len(volcanoes_db)} entries)")
            else:
                print(f"⚠️ No data available for volcanoes_db")

            if erupting_df is not None and not erupting_df.empty:
                erupting_df = erupting_df.merge(alerts_df, on="Name", how="left")
                erupting_df["date"] = date_str
                erupting_df.to_csv(data_paths["erupting"], index=False)
                ensure_table_exists(postgres_hook, f'erupting_volcanoes_{date_str.replace("-", "")}', erupting_df)
                insert_rows(postgres_hook, f'erupting_volcanoes_{date_str.replace("-", "")}', erupting_df)
                ensure_table_exists(postgres_hook, f'erupting_volcanoes_latest', erupting_df)
                insert_rows(postgres_hook, f'erupting_volcanoes_latest', erupting_df)
                params = erupting_df['Name'].dropna().unique().tolist()
                if params:
                    placeholders = ', '.join(['%s'] * len(params))
                    query = f"""
                        SELECT *
                        FROM volcanoes_db
                        WHERE LOWER("Volcano_Name") IN ({placeholders})
                    """
                    # Convert params to lowercase for case-insensitive matching
                    params_lower = [name.lower() for name in params]
                    filter_erupting_df = pd.read_sql(query, postgres_hook.get_conn(), params=params_lower)
                else:
                    query = "SELECT * FROM volcanoes_db"
                    filter_erupting_df = pd.read_sql(query, postgres_hook.get_conn())
                filter_erupting_df["date"] = date_str
                ensure_table_exists(postgres_hook, f'filtered_erupting_volcanoes_{date_str.replace("-", "")}', filter_erupting_df)
                insert_rows(postgres_hook, f'filtered_erupting_volcanoes_{date_str.replace("-", "")}', filter_erupting_df)
                ensure_table_exists(postgres_hook, f'filtered_erupting_volcanoes_latest', filter_erupting_df)
                insert_rows(postgres_hook, f'filtered_erupting_volcanoes_latest', filter_erupting_df)

                print(f"✅ erupting_volcanoes saved to {data_paths["erupting"]} ({len(erupting_df)} entries)")
            else:
                print(f"⚠️ No data available for erupting_volcanoes")

            if unrest_df is not None and not unrest_df.empty:
                unrest_df = unrest_df.merge(alerts_df, on="Name", how="left")
                unrest_df["date"] = date_str
                unrest_df.to_csv(data_paths["unrest"], index=False)
                ensure_table_exists(postgres_hook, f'unrest_volcanoes_{date_str.replace("-", "")}', unrest_df)
                insert_rows(postgres_hook, f'unrest_volcanoes_{date_str.replace("-", "")}', unrest_df)
                ensure_table_exists(postgres_hook, f'unrest_volcanoes_latest', unrest_df)
                insert_rows(postgres_hook, f'unrest_volcanoes_latest', unrest_df)
                print(f"✅ unrest_volcanoes saved to {data_paths["unrest"]} ({len(unrest_df)} entries)")
                params = unrest_df['Name'].dropna().unique().tolist()
                if params:
                    placeholders = ', '.join(['%s'] * len(params))
                    query = f"""
                        SELECT *
                        FROM volcanoes_db
                        WHERE LOWER("Volcano_Name") IN ({placeholders})
                    """
                    # Convert params to lowercase for case-insensitive matching
                    params_lower = [name.lower() for name in params]
                    filter_unrest_df = pd.read_sql(query, postgres_hook.get_conn(), params=params_lower)
                else:
                    query = "SELECT * FROM volcanoes_db"
                    filter_unrest_df = pd.read_sql(query, postgres_hook.get_conn())
                filter_unrest_df["date"] = date_str
                ensure_table_exists(postgres_hook, f'filtered_unrest_volcanoes_{date_str.replace("-", "")}', filter_unrest_df)
                insert_rows(postgres_hook, f'filtered_unrest_volcanoes_{date_str.replace("-", "")}', filter_unrest_df)
                ensure_table_exists(postgres_hook, f'filtered_unrest_volcanoes_latest', filter_unrest_df)
                insert_rows(postgres_hook, f'filtered_unrest_volcanoes_latest', filter_unrest_df)
            else:
                print(f"⚠️ No data available for unrest_volcanoes")

            if alerts_df is not None and not alerts_df.empty:
                alerts_df.to_csv(data_paths["alerts"], index=False)
                alerts_df["date"] = date_str
                ensure_table_exists(postgres_hook, f'alerts_volcanoes_{date_str.replace("-", "")}', alerts_df)
                insert_rows(postgres_hook, f'alerts_volcanoes_{date_str.replace("-", "")}', alerts_df)
                ensure_table_exists(postgres_hook, f'alerts_volcanoes_latest', alerts_df)
                insert_rows(postgres_hook, f'alerts_volcanoes_latest', alerts_df)
                print(f"✅ alerts_df saved to {data_paths["alerts"]} ({len(alerts_df)} entries)")
            else:
                print(f"⚠️ No data available for volcanoes_db")


            if eruptions_db is not None and not eruptions_db.empty:
                eruptions_db.to_csv(data_paths["historical_eruptions_db"], index=False)
                ensure_table_exists(postgres_hook, f'historical_eruptions_db', eruptions_db)
                insert_rows(postgres_hook, f'historical_eruptions_db', eruptions_db)
                print(f"✅ eruptions_db saved to {data_paths["historical_eruptions_db"]} ({len(eruptions_db)} entries)")
            else:
                print(f"⚠️ No data available for eruption_db")

            if earthquakes_db is not None and not earthquakes_db.empty:
                earthquakes_db.to_csv(data_paths["earthquakes_db"], index=False)
                earthquakes_db["date"] = date_str
                ensure_table_exists(postgres_hook, f'earthquakes_db_{date_str.replace("-", "")}', earthquakes_db)
                insert_rows(postgres_hook, f'earthquakes_db_{date_str.replace("-", "")}', earthquakes_db)
                ensure_table_exists(postgres_hook, f'earthquakes_db_latest', earthquakes_db)
                insert_rows(postgres_hook, f'earthquakes_db_latest', earthquakes_db)
                print(f"✅ earthquakes_db saved to {data_paths["earthquakes_db"]} ({len(earthquakes_db)} entries)")
            else:
                print(f"⚠️ No data available for earthquakes_db")


        erupting_df, unrest_df = scrape_volcanic_db()

        alerts_df = scrape_volcano_reports_alerts()

        volcanoes_db = download_wfs_points_to_csv(
            wfs_url="https://webservices.volcano.si.edu/geoserver/ows",
            typename="GVP-VOTW:Smithsonian_VOTW_Holocene_Volcanoes",
        )

        eruptions_db = download_wfs_points_to_csv(
            wfs_url="https://webservices.volcano.si.edu/geoserver/ows",
            typename="GVP-VOTW:Smithsonian_VOTW_Holocene_Eruptions",
        )

        earthquakes_db = scrape_earthquake_data()

        earthquakes_db["date"] = "2025-11-24"
        #earthquakes_db["date"] = datetime.today().date()

        get_data(erupting_df, unrest_df, alerts_df, volcanoes_db, eruptions_db, earthquakes_db)

    @task
    def transform_data_smithsonian():

        def query_database():
            postgres_hook = PostgresHook(postgres_conn_id="volcanic_etl")
            query_erupting_unrest = """
                    SELECT
                        *,
                        'erupting'::text AS source,
                        30::int AS buffer_km,
                        ST_Transform(
                            ST_Buffer(
                                ST_Transform(
                                    ST_SetSRID(ST_MakePoint(x_coordinate, y_coordinate), 4326),
                                    3857
                                ),
                                30000
                            ),
                            4326
                        ) AS geom_buffer
                    FROM filtered_erupting_volcanoes_latest

                    UNION ALL

                    SELECT
                        *,
                        'unrest'::text AS source,
                        30::int AS buffer_km,
                        ST_Transform(
                            ST_Buffer(
                                ST_Transform(ST_SetSRID(ST_MakePoint(v.x_coordinate, v.y_coordinate), 4326), 3857),
                                30000
                            ),
                            4326
                        ) AS buffer_geom
                    FROM filtered_unrest_volcanoes_latest v
            """
            print("Loading erupting volcanoes with buffers...")
            result_erupting_unrest = gpd.read_postgis(
                query_erupting_unrest,
                postgres_hook.get_conn(),
                geom_col='geom_buffer'
            )
            print(f"Successfully loaded {len(result_erupting_unrest)} erupting volcanoes with buffers")

            query_population_at_risk = """
                    WITH volcano_buffers AS (
                      SELECT
                          *,
                          'erupting'::text AS source,
                          30::int AS buffer_km,
                          ST_Transform(
                            ST_Buffer(
                              ST_Transform(ST_SetSRID(ST_MakePoint(x_coordinate, y_coordinate), 4326), 3857),
                              30000
                            ),
                            4326
                          ) AS buffer_geom
                      FROM filtered_erupting_volcanoes_latest 

                      UNION 

                      SELECT
                          *,
                          'unrest'::text AS source,
                          30::int AS buffer_km,
                          ST_Transform(
                            ST_Buffer(
                              ST_Transform(ST_SetSRID(ST_MakePoint(v.x_coordinate, v.y_coordinate), 4326), 3857),
                              30000
                            ),
                            4326
                          ) AS buffer_geom
                      FROM filtered_unrest_volcanoes_latest v
                    )
                    SELECT p.*, vb.id AS volcano_id, vb.source, vb.buffer_km
                    FROM population_centroid p
                    JOIN volcano_buffers vb
                      ON ST_Intersects(p.geom, vb.buffer_geom);
                """
            print("Finding population centroids within volcano buffers...")
            population_at_risk = gpd.read_postgis(
                query_population_at_risk,
                postgres_hook.get_conn(),
                geom_col='geom'  # Assuming population_centroid has x/y coordinates
            )
            print(f"Found {len(population_at_risk)} population centroids at risk")

            query_alert = """
                SELECT *
                FROM alerts_volcanoes_latest
            """
            print("Loading alerts volcanoes...")
            result_alert = pd.read_sql(query_alert, postgres_hook.get_conn())
            print(f"Successfully loaded {len(result_alert)} unrest volcanoes")

            query_db = """
                SELECT *
                FROM volcanoes_db
            """
            print("Loading main volcanoes database...")
            result_db = pd.read_sql(query_db, postgres_hook.get_conn())
            print(f"Successfully loaded {len(result_db)} volcanoes from main database")

            query_historical = """
                SELECT *
                FROM "MOESM1"
            """
            print("Loading historical data (MOESM1)...")
            historical_db = pd.read_sql(query_historical, postgres_hook.get_conn())
            print(f"Successfully loaded {len(historical_db)} records from MOESM1")

            query_historical_gvp = """
                SELECT *
                FROM "historical_eruptions_db"
            """
            print("Loading historical eruptions (GVP)...")
            historical_db_GVP = pd.read_sql(query_historical_gvp, postgres_hook.get_conn())
            print(f"Successfully loaded {len(historical_db_GVP)} records from historical_eruptions_db")

            query_earthquakes = """
                SELECT *
                FROM "earthquakes_db_latest"
            """
            print("Loading earthquakes data ...")
            earthquakes_db = pd.read_sql(query_earthquakes, postgres_hook.get_conn())
            print(f"Successfully loaded {len(earthquakes_db)} records")

            if len(population_at_risk) > 0:
                print("\n=== Population at Risk Analysis ===")
                print(f"Total population centers at risk: {len(population_at_risk)}")

                if 'pop' in population_at_risk.columns:
                    total_affected = population_at_risk['pop'].sum()
                    print(f"Total population at risk: {total_affected:,}")

                    print("\nTop 5 most affected population centers:")
                    print(population_at_risk.nlargest(5, 'pop')[['pop', 'gid']])

                if 'volcano_id' in population_at_risk.columns:
                    print("\nPopulation at risk by volcano:")
                    risk_by_volcano = population_at_risk.groupby('volcano_id').size().reset_index(name='centers_affected')
                    print(risk_by_volcano)

                return result_erupting_unrest, result_alert, result_db, historical_db, historical_db_GVP, population_at_risk, total_affected, risk_by_volcano, earthquakes_db

        def request_osm(spatial_boundingbox, list_tags):
            try:
                results_quering = ox.features_from_bbox(
                    bbox=spatial_boundingbox,
                    tags=list_tags
                )
                if results_quering is None:
                    print("Warning: No data returned (None)")
                else:
                    return results_quering

            except Exception as e:
                print(f"Warning: Could not load data from OSM- {str(e)}")

        def linestring_to_coords(geom):
            if geom.geom_type == "LineString":
                return {"path": [[x, y] for x, y in geom.coords]}
            elif geom.geom_type == "MultiLineString":
                return {"path": [[[x, y] for x, y in line.coords] for line in geom.geoms]}
            else:
                return None

        def convert_pop_dataframe(spdf):
            try:
                if 'geom' in spdf.columns:
                    spdf['geom'] = spdf['geom'].apply(
                        lambda x: wkb.loads(x, hex=True) if x else None)
                    pop_df = gpd.GeoDataFrame(spdf, geometry='geom', crs="EPSG:4326")  # Adjust CRS as needed
                    return pop_df
                else:
                    print("Warning: No data returned (None)")

            except Exception as e:
                print(f"Warning: Could not load pop data - {str(e)}")

        def spatial_analysis(volcano, pop_db):

            geodataframe = gpd.GeoDataFrame(volcano, geometry='geom_buffer', crs="EPSG:4326")

            bbox = geodataframe.total_bounds  # [minx, miny, maxx, maxy]

            tags_emergency_service = {
                'amenity': [
                    'fire_station',
                    'police',
                    'hospital',
                    'ambulance_station',
                ]
            }

            tags_essential_service = {
                'amenity': [
                    'supermarket', 'fuel', 'chemist',
                    'shelter',
                    'Pharmacy', 'dentist', 'doctors', 'embassy', 'townhall', 'courthouse', 'veterinary'
                ]
            }

            tags_amenity = {
                'amenity': [
                    'kindergarten', 'school', 'library', 'college', 'university', 'prison', 'social_facility',
                    'nursing_home',
                ]
            }

            tags_roads = {
                'highway': [
                    'motorway', 'motorway link', 'trunk', 'trunk link', 'primary', 'primary link', 'secondary',
                    'secondary link', 'tertiary', 'tertiary link',
                    'unclassified', 'residential', 'living street', 'service', 'road', 'unknown'
                ]
            }

            warnings.filterwarnings("ignore", message="Geometry is in a geographic CRS.*")

            print('+++ emergency_service')
            emergency_service = request_osm(bbox, tags_emergency_service)
            if isinstance(emergency_service, gpd.GeoDataFrame):
                emergency_service = emergency_service.to_crs("EPSG:4326")
                emergency_service.geometry = emergency_service.geometry.centroid

            print('+++ essential_service')
            essential_service = request_osm(bbox, tags_essential_service)
            if isinstance(essential_service, gpd.GeoDataFrame):
                essential_service = essential_service.to_crs("EPSG:4326")
                essential_service.geometry = essential_service.geometry.centroid

            print('+++ amenity')
            amenity = request_osm(bbox, tags_amenity)
            if isinstance(amenity, gpd.GeoDataFrame):
                amenity = amenity.to_crs("EPSG:4326")
                amenity.geometry = amenity.geometry.centroid

            print('+++ roads')
            roads = request_osm(bbox, tags_roads)
            if isinstance(roads, gpd.GeoDataFrame):
                roads = roads.to_crs("EPSG:4326")

            print('+++ graph')
            print('+++      download graph')
            custom_filter = '["highway"~"motorway|trunk|primary|secondary|tertiary"]'
            graph = ox.graph_from_bbox(bbox, network_type="drive", custom_filter=custom_filter)
            if graph and len(graph.nodes()) > 0:
                graph_proj = ox.project_graph(graph)
                nodes_proj, edges_proj = ox.graph_to_gdfs(graph_proj, nodes=True, edges=True)
                print(f'+++      betweenness_centrality - {len(nodes_proj)} nodes')
                betweenness_centrality = nx.betweenness_centrality(graph_proj)
                nodes_proj['betweenness_centrality'] = nodes_proj.index.map(betweenness_centrality)
                nodes_proj = nodes_proj.to_crs(pop_db.crs)
                print('+++      population join')
                population_clipped = gpd.clip(pop_db, bbox)
                nodes_proj = gpd.sjoin_nearest(nodes_proj, population_clipped, distance_col="distances",
                                               lsuffix="left", rsuffix="right", exclusive=True)
                nodes_proj = nodes_proj[['betweenness_centrality', 'pop', 'distances', 'geometry']]

                print('+++      score')
                nodes_proj['pop_norm'] = (
                        (nodes_proj['pop'] - nodes_proj['pop'].min()) /
                        (nodes_proj['pop'].max() - nodes_proj['pop'].min())
                )

                nodes_proj['betweenness_norm'] = (
                        (nodes_proj['betweenness_centrality'] - nodes_proj['betweenness_centrality'].min()) /
                        (nodes_proj['betweenness_centrality'].max() - nodes_proj['betweenness_centrality'].min())
                )

                nodes_proj['score'] = (
                        0.5 * nodes_proj['pop_norm'] +
                        0.5 * nodes_proj['betweenness_norm']
                )

            return roads, emergency_service, amenity, essential_service, nodes_proj

        result_erupting_unrest, result_alerts, result_db, historical_db, historical_db_GVP, population_at_risk, total_affected, risk_by_volcano, earthquakes_db = query_database()

        if result_erupting_unrest is not None:
            data_paths = {
                "erupting_unrest": '/home/gillet/Bureau/Volcanic_ETL/ETL/app/data/erupting_unrest_volcanoes_latest.csv',
                "roads": '/home/gillet/Bureau/Volcanic_ETL/ETL/app/data/osm_highways_features.pkl',
                "emergency": '/home/gillet/Bureau/Volcanic_ETL/ETL/app/data/all_emergency_services.gpkg',
                "amenities": '/home/gillet/Bureau/Volcanic_ETL/ETL/app/data/all_amenities.gpkg',
                "essential_services": '/home/gillet/Bureau/Volcanic_ETL/ETL/app/data/all_essential_services.gpkg',
                "all_nodes": '/home/gillet/Bureau/Volcanic_ETL/ETL/app/data/all_nodes.gpkg',
                "alerts_volcanoes_latest": '/home/gillet/Bureau/Volcanic_ETL/ETL/app/data/alerts_volcanoes_latest.csv',
                "volcanoes_db": '/home/gillet/Bureau/Volcanic_ETL/ETL/app/data/volcanoes_db.csv',
                "historical_db": '/home/gillet/Bureau/Volcanic_ETL/ETL/app/data/historical_db.csv',
                "historical_db_GVP": '/home/gillet/Bureau/Volcanic_ETL/ETL/app/data/historical_db_GVP.csv',
                "population_at_risk": '/home/gillet/Bureau/Volcanic_ETL/ETL/app/data/population_at_risk.csv',
                "total_affected": '/home/gillet/Bureau/Volcanic_ETL/ETL/app/data/total_affected.csv',
                "risk_by_volcano": '/home/gillet/Bureau/Volcanic_ETL/ETL/app/data/risk_by_volcano.csv',
                "earthquakes_db": '/home/gillet/Bureau/Volcanic_ETL/ETL/app/data/earthquakes_db.csv'
            }

            test_df = result_erupting_unrest.drop(columns=['geom_buffer'])
            test_path = '/home/gillet/Bureau/Volcanic_ETL/ETL/app/data/erupting_unrest.csv'

            try:
                test_df.to_csv(test_path, index=False, encoding='utf-8')
                print("✅ UTF-8 save successful for test data")
            except Exception as e:
                print(f"❌ UTF-8 save failed: {e}")
                # Try latin1
                test_df.to_csv(test_path, index=False, encoding='latin1')
                print("⚠️ Saved with latin1 instead")

            # Try reading back
            try:
                pd.read_csv(test_path, encoding='utf-8')
                print("✅ UTF-8 read successful")
            except Exception as e:
                print(f"❌ UTF-8 read failed: {e}")
                pd.read_csv(test_path, encoding='latin1')

            all_roads = []
            all_emergency_services = []
            all_amenities = []
            all_essential_services = []
            all_nodes = []

            for idx, volcan in result_erupting_unrest.iterrows():
                try:
                    print(f"Processing volcano: {volcan['Volcano_Name']} ({idx + 1}/{len(result_erupting_unrest)})")

                    roads, emergency_services, amenities, essential_services, nodes = spatial_analysis(volcano=volcan.to_frame().T, pop_db = population_at_risk)

                    if isinstance(roads, gpd.GeoDataFrame):
                        roads['volcano_name'] = volcan['Volcano_Name']
                        roads['id'] = volcan['id']
                        roads['region'] = volcan['Region']
                        all_roads.append(roads)

                    if isinstance(emergency_services, gpd.GeoDataFrame):
                        emergency_services['volcano_name'] = volcan['Volcano_Name']
                        emergency_services['id'] = volcan['id']
                        emergency_services['region'] = volcan['Region']
                        all_emergency_services.append(emergency_services)

                    if isinstance(amenities, gpd.GeoDataFrame):
                        amenities['volcano_name'] = volcan['Volcano_Name']
                        amenities['id'] = volcan['id']
                        amenities['region'] = volcan['Region']
                        all_amenities.append(amenities)

                    if isinstance(essential_services, gpd.GeoDataFrame):
                        essential_services['volcano_name'] = volcan['Volcano_Name']
                        essential_services['id'] = volcan['id']
                        essential_services['region'] = volcan['Region']
                        all_essential_services.append(essential_services)

                    if isinstance(nodes, gpd.GeoDataFrame):
                        nodes['volcano_name'] = volcan['Volcano_Name']
                        nodes['id'] = volcan['id']
                        nodes['region'] = volcan['Region']
                        all_nodes.append(nodes)

                    print(f"Completed processing for {volcan['Volcano_Name']}")

                except Exception as e:
                    print(f"Error processing {volcan['Volcano_Name']}: {str(e)}")
                    continue

                if len(all_roads) != 0:

                    final_roads = gpd.GeoDataFrame(pd.concat(all_roads, ignore_index=True))

                    osm_highway_colors = {
                        "motorway": [255, 0, 0],  # Red (unchanged)
                        "trunk": [255, 128, 0],  # Orange
                        "primary": [255, 255, 0],  # Yellow
                        "secondary": [128, 255, 0],  # Light green
                        "tertiary": [200, 200, 200, 128],  # Light gray with transparency (RGBA)
                        "unclassified": [255, 255, 255],  # White
                        "residential": [220, 220, 220],  # Light gray
                        "service": [192, 192, 192],  # Darker gray
                        "path": [0, 128, 0],  # Dark green
                        "footway": [128, 0, 128],  # Purple
                        "cycleway": [0, 128, 128],  # Teal
                        "bridleway": [128, 0, 0],  # Dark red
                        "steps": [0, 0, 128],  # Navy
                    }

                    highway_widths = {
                        "motorway": 8,
                        "trunk": 7,
                        "primary": 6,
                        "secondary": 5,
                        "tertiary": 4,
                    }

                    final_roads["color"] = final_roads["highway"].apply(
                        lambda x: osm_highway_colors.get(x, [128, 128, 128])  # Default: gray
                    )

                    final_roads["width"] = final_roads["highway"].apply(
                        lambda x: highway_widths.get(x, 3)  # Default: gray
                    )

                    features = []
                    allowed_highways = {"motorway", "trunk", "primary", "secondary", "tertiary"}

                    for _, row in final_roads.iterrows():
                        coords = linestring_to_coords(row.geometry)
                        if coords and row["highway"] in allowed_highways:  # Check if highway is allowed
                            features.append({
                                **coords,
                                "color": row["color"],
                                "width": row["width"],
                                "highway_type": row["highway"],
                                "name": row.get("name", ""),
                                "id": row["id"],
                                "volcano_name": row["volcano_name"],
                            })

                    with open(data_paths["roads"], "wb") as f:
                        pickle.dump(features, f)

                if len(all_emergency_services) != 0:
                    final_emergency_services = gpd.GeoDataFrame(pd.concat(all_emergency_services, ignore_index=True))
                    final_emergency_services[['geometry', 'id', 'amenity']].to_file(data_paths["emergency"], driver='GPKG')
                if len(all_amenities) != 0:
                    final_amenities = gpd.GeoDataFrame(pd.concat(all_amenities, ignore_index=True))
                    final_amenities[['geometry', 'id', 'amenity']].to_file(data_paths["amenities"], driver='GPKG')
                if len(all_essential_services) != 0:
                    final_essential_services = gpd.GeoDataFrame(pd.concat(all_essential_services, ignore_index=True))
                    final_essential_services[['geometry', 'id', 'amenity']].to_file(data_paths["essential_services"], driver='GPKG')
                if len(all_nodes) != 0:
                    final_nodes = gpd.GeoDataFrame(pd.concat(all_nodes, ignore_index=True))
                    final_nodes[['geometry', 'id', 'score']].to_file(data_paths["all_nodes"], driver='GPKG')

                print("All volcanoes processed successfully!")

            if result_alerts is not None:
                result_alerts.to_csv(data_paths["alerts_volcanoes_latest"], index=False)

            if result_db is not None:
                result_db.to_csv(data_paths["volcanoes_db"], index=False)

            if historical_db is not None:
                population_cols = [col for col in historical_db.columns if
                                   col.strip().lower().startswith("population")]
                for col in population_cols:
                    historical_db[col] = (
                        historical_db[col]
                        .astype(str)  # Ensure string type for .str operations
                        .str.replace(",", "", regex=False)  # Remove commas
                        .str.strip()  # Remove whitespace
                    )
                    historical_db[col] = pd.to_numeric(historical_db[col], errors="coerce")

                    if historical_db[col].dropna().mod(1).eq(0).all():  # Check if all values are whole numbers
                        historical_db[col] = historical_db[col].astype("Int64")

                historical_db.to_csv(data_paths["historical_db"], index=False)

            if historical_db_GVP is not None:
                historical_db_GVP.to_csv(data_paths["historical_db_GVP"], index=False)

            if population_at_risk is not None:
                population_at_risk.to_csv(data_paths["population_at_risk"], index=False)

            if total_affected is not None:
                pd.DataFrame({'total_affected': [total_affected]}).to_csv(data_paths["total_affected"], index=False)

            if risk_by_volcano is not None:
                risk_by_volcano.to_csv(data_paths["risk_by_volcano"], index=False)

            if earthquakes_db is not None:
                earthquakes_db.to_csv(data_paths["earthquakes_db"], index=False)

    @task
    def load_data_smithsonian():

        def git_push(
                repo_path: str,
                commit_message: str,
                branch: str = "main",
                remote: str = "origin",
                git_username: Optional[str] = None,
                git_email: Optional[str] = None,
                github_token: Optional[str] = None
        ) -> Tuple[bool, str]:
            """
            Push changes to a Git repository.

            Args:
                repo_path: Path to the local git repository
                commit_message: Commit message for the changes
                branch: Branch to push to (default: main)
                remote: Remote name (default: origin)
                git_username: Git username for configuration (optional)
                git_email: Git email for configuration (optional)
                github_token: GitHub personal access token for authentication (optional)

            Returns:
                Tuple of (success: bool, output: str)
            """
            try:
                # Change to repository directory
                os.chdir(repo_path)

                # Configure git if credentials provided
                if git_username and git_email:
                    subprocess.run(["git", "config", "user.name", git_username], check=True)
                    subprocess.run(["git", "config", "user.email", git_email], check=True)

                # Add all changes
                add_result = subprocess.run(["git", "add", "."], capture_output=True, text=True)
                if add_result.returncode != 0:
                    return False, f"Failed to add changes: {add_result.stderr}"

                # Commit changes
                commit_result = subprocess.run(
                    ["git", "commit", "-m", commit_message],
                    capture_output=True,
                    text=True
                )
                if commit_result.returncode != 0:
                    return False, f"Failed to commit changes: {commit_result.stderr}"

                # Push changes
                push_cmd = ["git", "push", remote, branch]
                if github_token:
                    # Use token for authentication
                    remote_url = subprocess.run(
                        ["git", "config", "--get", f"remote.{remote}.url"],
                        capture_output=True,
                        text=True
                    ).stdout.strip()

                    if "https://" in remote_url:
                        # For HTTPS URLs
                        auth_url = remote_url.replace(
                            "https://",
                            f"https://{github_token}@"
                        )
                        push_cmd.extend(["--repo", auth_url])
                    else:
                        # For SSH URLs - this is more complex and might need ssh-agent setup
                        pass

                push_result = subprocess.run(push_cmd, capture_output=True, text=True)

                if push_result.returncode != 0:
                    return False, f"Failed to push changes: {push_result.stderr}"

                return True, f"Successfully pushed changes to {branch} branch"

            except Exception as e:
                return False, f"Error during git operations: {str(e)}"

        def push_to_git(**context):
            """
            Airflow task to push changes to Git repository.
            """

            repo_path = Variable.get("GIT_REPO_PATH", default_var="/path/to/your/repository")
            branch = Variable.get("GIT_BRANCH", default_var="main")
            git_username = Variable.get("GIT_USERNAME")
            git_email = Variable.get("GIT_EMAIL")
            github_token = Variable.get("GITHUB_TOKEN")

            date_obj = datetime.now()
            date_obj = date_obj - timedelta(days=1)
            #date_str = "2025-11-24"
            date_str = date_obj.strftime("%Y-%m-%d")
            commit_message = f"Automated commit by Airflow at {date_str}"

            # Call the git push function
            success, message = git_push(
                repo_path=repo_path,
                commit_message=commit_message,
                branch=branch,
                git_username=git_username,
                git_email=git_email,
                github_token=github_token
            )

            if not success:
                raise Exception(f"Git push failed: {message}")

            print(message)
            return message

        push_to_git()

    extract_data_smithsonian() >> transform_data_smithsonian() >> load_data_smithsonian()

dag = process_data_smithsonian()