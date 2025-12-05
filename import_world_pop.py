import geopandas as gpd
from sqlalchemy import create_engine

# Read the GeoPackage
gdf = gpd.read_file('/home/gillet/pop.gpkg')

# Create connection
engine = create_engine('postgresql://oliviergillet:@localhost:5432/volcanic_etl')

# Write to PostGIS (in chunks for large files)
chunk_size = 10000
for i in range(0, len(gdf), chunk_size):
    gdf[i:i+chunk_size].to_postgis(
        'table_name',
        engine,
        schema='schema_name',
        if_exists='append' if i > 0 else 'replace',
        index=False,
        dtype={'geometry': 'geometry(Geometry, 4326)'}
    )