import requests
from bs4 import BeautifulSoup
import pandas as pd
import geopandas as gpd
from datetime import datetime
import argparse
from pathlib import Path
from owslib.wfs import WebFeatureService
import re

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
        print(df.head())

        return df

    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching the webpage: {e}")
        return None, None
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
        return None, None

def scrape_volcano_data(date_str):
    """
    Scrapes volcano eruption and unrest data for a given date.

    Args:
        date_str (str): Date in 'YYYY-MM-DD' format (e.g., '2025-09-15').

    Returns:
        tuple: (DataFrame of erupting volcanoes, DataFrame of unrest volcanoes)
    """
    # Build the target URL
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
        formatted_date = datetime.strptime(date_str, '%Y-%m-%d').strftime('%d %B %Y')
        eruption_title = f"List of Volcanoes with Eruptive Activity on {formatted_date}"
        unrest_title = f"List of Volcanoes with Unrest on {formatted_date}"

        eruption_df = extract_section(eruption_title)
        unrest_df = extract_section(unrest_title)

        return eruption_df, unrest_df

    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching the webpage: {e}")
        return None, None
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
        return None, None


def scrape_volcano_reports_alerts(date_str):
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
                    text = re.sub(r"\s+", " ", right_td.get_text(" ", strip=True)).strip()
                    m_obs = re.search(r"Observatory Alert Level:\s*\"?([^\"(]+)\"?", text, re.I)
                    m_avn = re.search(r"Aviation Alert Level:\s*\"?([^\"(]+)\"?:", text, re.I)
                    obs_level = m_obs.group(1).strip() if m_obs else None
                    avn_level = m_avn.group(1).strip() if m_avn else None
                    if not avn_level:
                        m_avn2 = re.search(r"aviation alert level.*?(?:to|at)\s*\"?([A-Za-z]+)\"?", text, re.I)
                        avn_level = m_avn2.group(1).strip() if m_avn2 else "unavailable or not collected"
                    if obs_level and re.search(r"unavailable|not collected", obs_level, re.I):
                        obs_level = "unavailable or not collected"

                rows.append({
                    "Name": name,
                    "observatory_level": obs_level,
                    "aviation_level": avn_level
                })

            df = pd.DataFrame(rows)
            return df

        formatted_date = datetime.strptime(date_str, '%Y-%m-%d').strftime('%d %B %Y')
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


def save_to_csv(df, filename, section_name, to_app = False):
    """Saves a DataFrame to a CSV file in the data directory."""
    if df is not None and not df.empty:
        # Get the absolute path to the data directory
        # Assuming this script is in ETL/collect.py
        script_dir = Path(__file__).parent  # ETL directory
        data_dir = script_dir.parent / "data"  # data directory (one level up)

        if to_app :
            data_dir = script_dir.parent / "ETL/app/data"  # data directory (one level up

        # Create data directory if it doesn't exist
        data_dir.mkdir(exist_ok=True)

        # Full path for the output file
        output_path = data_dir / filename

        df.to_csv(output_path, index=False)
        print(f"✅ {section_name} saved to {output_path} ({len(df)} entries)")
    else:
        print(f"⚠️ No data available for {section_name}")

if __name__ == '__main__':
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(
        description='Scrape volcano data for a specific date.'
    )
    parser.add_argument('date', type=str, help='Date in YYYY-MM-DD format (e.g., 2025-09-15)')
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, '%Y-%m-%d')
    except ValueError:
        print("❌ Invalid date format. Use YYYY-MM-DD (e.g., 2025-09-15).")
        exit(1)

    eruption_df, unrest_df = scrape_volcano_data(args.date)
    alerts_df = scrape_volcano_reports_alerts(args.date)

    if eruption_df is not None:
        eruption_df = eruption_df.merge(alerts_df, on="Name", how="left")

    if unrest_df is not None:
        unrest_df = unrest_df.merge(alerts_df, on="Name", how="left")

    # Request geoserver
    volcano_db = download_wfs_points_to_csv(
        wfs_url="https://webservices.volcano.si.edu/geoserver/ows",
        typename="GVP-VOTW:Smithsonian_VOTW_Holocene_Volcanoes",
    )

    eruption_db = download_wfs_points_to_csv(
        wfs_url="https://webservices.volcano.si.edu/geoserver/ows",
        typename="GVP-VOTW:Smithsonian_VOTW_Holocene_Eruptions",
    )

    list_daily_update = []
    # Save results to CSV files in the ../data/ directory
    if eruption_df is not None:
        save_to_csv(eruption_df, f'erupting_volcanoes_latest.csv', "erupting_volcanoes")
        print("\n=== ERUPTING VOLCANOES ===")
        print(eruption_df.to_string(index=False))
        list_daily_update.append("erupting_volcanoes")

    if unrest_df is not None:
        save_to_csv(unrest_df, f'unrest_volcanoes_latest.csv', "unrest_volcanoes")
        print("\n=== UNREST VOLCANOES ===")
        print(unrest_df.to_string(index=False))
        list_daily_update.append("unrest_volcanoes")

    if alerts_df is not None:
        save_to_csv(alerts_df, f'alerts_volcanoes_latest.csv', "alerts_volcanoes")
        print("\n=== UNREST VOLCANOES ===")
        print(alerts_df.to_string(index=False))
        list_daily_update.append("alerts_volcanoes")

    if volcano_db is not None:
        print("\n=== WORLD VOLCANOES DB ===")
        save_to_csv(volcano_db, f'world_actives_volcanoes_db.csv', "volcanoes_db")
        list_daily_update.append("volcanoes_db")

    if eruption_db is not None:
        print("\n=== HISTORICAL ERUPTIONS DB ===")
        save_to_csv(eruption_db, f'historical_eruptions_db.csv', "eruptions_db")
        list_daily_update.append("volcanoes_db")

    earth_quake_db = scrape_earthquake_data()

    if earth_quake_db is not None:
        print("\n=== Earthquakes DB ===")
        save_to_csv(earth_quake_db, f'earth_quake_db.csv', "earth_quake_db")
        list_daily_update.append("earth_quake_db")


    data = {
        'information': ['date'],
        'value': [args.date],
        'colleted_data': str(list_daily_update)
    }
    df_informations = pd.DataFrame(data)
    save_to_csv(df_informations, f'information_etl.csv', "informations", to_app=True)

