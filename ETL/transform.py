import os
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go
from collect import save_to_csv
from load import load_geodataframe_to_postgis
import geopandas as gpd
from shapely.geometry import Point, box
import osmnx as ox
import numpy as np
import networkx as nx
from pathlib import Path
import warnings
from shapely import wkb
import pickle

warnings.filterwarnings("ignore", message="Geometry is in a geographic CRS.*")
warnings.filterwarnings("ignore", message=" Geometry column does not*")

def get_db_connection():
    """Create and return a PostgreSQL database connection and SQLAlchemy engine"""
    db_url = f"postgresql://{os.getenv('DB_USER', 'oliviergillet')}:{os.getenv('DB_PASSWORD', 'volcanic')}@" \
             f"{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/" \
             f"{os.getenv('DB_NAME', 'volcanic_etl')}"

    engine = create_engine(db_url)
    conn = engine.connect()
    return conn, engine

import os

def delete_files_in_folder(folder_path):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                # If you want to delete subdirectories too, use shutil.rmtree()
                pass
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')

def query_database():
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
            FROM erupting_volcanoes_latest
            
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
            FROM unrest_volcanoes_latest v
    """

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
          FROM erupting_volcanoes_latest 
        
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
          FROM unrest_volcanoes_latest v
        )
        SELECT p.*, vb.id AS volcano_id, vb.source, vb.buffer_km
        FROM population_centroid p
        JOIN volcano_buffers vb
          ON ST_Intersects(p.geom, vb.buffer_geom);
    """

    query_unrest = """
        SELECT *
        FROM unrest_volcanoes_latest
    """

    query_alert = """
        SELECT *
        FROM alerts_volcanoes_latest
    """

    query_db = """
        SELECT *
        FROM volcanoes_db
    """

    query_historical = """
        SELECT *
        FROM "MOESM1"
    """

    query_historical_gvp = """
        SELECT *
        FROM "historical_eruptions_db"
    """

    try:
        conn.rollback()

        print("Loading erupting volcanoes with buffers...")
        result_erupting_unrest = gpd.read_postgis(
            query_erupting_unrest,
            conn,
            geom_col='geom_buffer'
        )
        print(f"Successfully loaded {len(result_erupting_unrest)} erupting volcanoes with buffers")

        print("Finding population centroids within volcano buffers...")
        population_at_risk = gpd.read_postgis(
            query_population_at_risk,
            conn,
            geom_col='geom'  # Assuming population_centroid has x/y coordinates
        )
        print(f"Found {len(population_at_risk)} population centroids at risk")
        print(population_at_risk.head())

        print("Loading alerts volcanoes...")
        result_alert = pd.read_sql(query_alert, conn)
        print(f"Successfully loaded {len(result_alert)} unrest volcanoes")

        print("Loading main volcanoes database...")
        result_db = pd.read_sql(query_db, conn)
        print(f"Successfully loaded {len(result_db)} volcanoes from main database")

        print("Loading historical data (MOESM1)...")
        historical_db = pd.read_sql(query_historical, conn)
        print(f"Successfully loaded {len(historical_db)} records from MOESM1")

        print("Loading historical eruptions (GVP)...")
        historical_db_GVP = pd.read_sql(query_historical_gvp, conn)
        print(f"Successfully loaded {len(historical_db_GVP)} records from historical_eruptions_db")

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

            return result_erupting_unrest, result_alert, result_db, historical_db, historical_db_GVP, population_at_risk, total_affected, risk_by_volcano

    except Exception as e:
        conn.rollback()
        print(f"Error executing queries: {e}")
        raise
    finally:
        pass

def request_osm(spatial_boundingbox, list_tags):
    try:
        results_quering = ox.features_from_bbox(
            bbox=spatial_boundingbox,
            tags=list_tags
        )
        if results_quering is None:
            print("Warning: No data returned (None)")
        else :
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
            pop_df = gpd.GeoDataFrame(spdf, geometry='geom', crs="EPSG:4326") # Adjust CRS as needed
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
            'supermarket','fuel','chemist',
            'shelter',
            'Pharmacy', 'dentist', 'doctors', 'embassy', 'townhall', 'courthouse', 'veterinary'
        ]
    }

    tags_amenity = {
        'amenity': [
            'kindergarten','school','library','college','university','prison','social_facility', 'nursing_home',
        ]
    }

    tags_roads = {
        'highway': [
            'motorway', 'motorway link','trunk', 'trunk link', 'primary', 'primary link', 'secondary', 'secondary link', 'tertiary', 'tertiary link',
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
        betweenness_centrality=nx.betweenness_centrality(graph_proj)
        nodes_proj['betweenness_centrality'] = nodes_proj.index.map(betweenness_centrality)
        nodes_proj = nodes_proj.to_crs(pop_db.crs)
        print('+++      population join')
        population_clipped = gpd.clip(pop_db, bbox)
        nodes_proj = gpd.sjoin_nearest(nodes_proj, population_clipped, distance_col="distances",
                          lsuffix="left", rsuffix="right", exclusive=True)
        nodes_proj = nodes_proj[['betweenness_centrality','pop', 'distances', 'geometry']]

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

if __name__ == "__main__":

    conn, engine = get_db_connection()
    try:

        folder_path = 'ETL/app/data'
        delete_files_in_folder(folder_path)

        result_erupting_unrest, result_alerts, result_db, historical_db, historical_db_GVP, population_at_risk, total_affected, risk_by_volcano = query_database()

        if result_erupting_unrest is not None:
            save_to_csv(result_erupting_unrest, f'erupting_unrest_volcanoes_latest.csv', "erupting volcanoes", to_app=True)

            # Initialize lists to store results
            all_roads = []
            all_emergency_services = []
            all_amenities = []
            all_essential_services = []
            all_buffer = []
            all_nodes = []

            #result_erupting_unrest = gpd.GeoDataFrame(pd.concat([result_erupting, result_unrest], ignore_index=True))

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
                output_path = Path('ETL/app/data/all_roads.gpkg')

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
                    lambda x: osm_highway_colors.get(x, [128, 128, 128])# Default: gray
                )

                final_roads["width"] = final_roads["highway"].apply(
                    lambda x: highway_widths.get(x, 3)# Default: gray
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

                with open("ETL/app/data/osm_highways_features.pkl", "wb") as f:  # 'wb' = write binary
                    pickle.dump(features, f)
                #final_roads[['geometry', 'id', 'highway']].to_file(output_path, driver='gpkg')
                #if final_roads is not None:
                    #load_geodataframe_to_postgis(engine, final_roads[['geometry', 'highway']], "roads")
            if len(all_emergency_services) != 0:
                final_emergency_services = gpd.GeoDataFrame(pd.concat(all_emergency_services, ignore_index=True))
                output_path = Path('ETL/app/data/all_emergency_services.gpkg')
                final_emergency_services[['geometry', 'id', 'amenity']].to_file(output_path, driver='GPKG')
                #if final_emergency_services is not None:
                    #load_geodataframe_to_postgis(engine, final_emergency_services[['geometry', 'amenity']], "emergency")
            if len(all_amenities) != 0:
                final_amenities = gpd.GeoDataFrame(pd.concat(all_amenities, ignore_index=True))
                output_path = Path('ETL/app/data/all_amenities.gpkg')
                final_amenities[['geometry', 'id', 'amenity']].to_file(output_path, driver='GPKG')
                #if final_amenities is not None:
                    #load_geodataframe_to_postgis(engine, final_amenities[['geometry', 'amenity']], "amenity")
            if len(all_essential_services) != 0:
                final_essential_services = gpd.GeoDataFrame(pd.concat(all_essential_services, ignore_index=True))
                output_path = Path('ETL/app/data/all_essential_services.gpkg')
                final_essential_services[['geometry', 'id', 'amenity']].to_file(output_path, driver='GPKG')
                #if final_essential_services is not None:
                    #load_geodataframe_to_postgis(engine, final_essential_services[['geometry', 'amenity']], "essential")
            if len(all_nodes) != 0:
                final_nodes = gpd.GeoDataFrame(pd.concat(all_nodes, ignore_index=True))
                output_path = Path('ETL/app/data/all_nodes.gpkg')
                final_nodes[['geometry', 'id','score']].to_file(output_path, driver='GPKG')
                #if final_essential_services is not None:
                    #load_geodataframe_to_postgis(engine, final_essential_services[['geometry', 'amenity']], "essential")

            print("All volcanoes processed successfully!")

        if result_alerts is not None:
            save_to_csv(result_alerts, f'alerts_volcanoes_latest.csv', "alerts volcanoes", to_app=True)

        if result_db is not None:
            save_to_csv(result_db, f'volcanoes_db.csv', "db volcanoes", to_app=True)

        if historical_db is not None:
            population_cols = [col for col in historical_db.columns if col.strip().lower().startswith("population")]
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
            save_to_csv(historical_db, f'historical_db.csv', "historical db", to_app=True)

        if historical_db_GVP is not None:
            save_to_csv(historical_db_GVP, f'historical_db_GVP.csv', "historical_db_GVP", to_app=True)

        if population_at_risk is not None:
            save_to_csv(population_at_risk, f'population_at_risk.csv', "population_at_risk", to_app=True)

        if total_affected is not None:
            save_to_csv(pd.DataFrame({'total_affected': [total_affected]}), f'total_affected.csv', "total_affected", to_app=True)

        if risk_by_volcano is not None:
            save_to_csv(risk_by_volcano, f'risk_by_volcano.csv', "risk_by_volcano", to_app=True)

    finally:
        conn.close()
        engine.dispose()





