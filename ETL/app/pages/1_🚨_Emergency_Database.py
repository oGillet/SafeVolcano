import pydeck
import pandas as pd
import streamlit as st
import geopandas as gpd
from shapely.geometry import Point
import pickle

pd.options.mode.chained_assignment = None  # default='warn'

st.markdown("""
<style>
    .main {
        background-color: #f8f9fa;
    }
    .stButton>button {
        background-color: #4CAF50;
        color: white;
        border: none;
        padding: 10px 24px;
        text-align: center;
        text-decoration: none;
        display: inline-block;
        font-size: 16px;
        margin: 4px 2px;
        cursor: pointer;
        border-radius: 8px;
    }
    .stButton>button:hover {
        background-color: #45a049;
    }
    .stSelectbox, .stMultiSelect, .stSlider {
        margin-bottom: 20px;
    }
    .header {
        color: #2c3e50;
        font-size: 2.5em;
        text-align: center;
        margin-bottom: 10px;
    }
    .subtitle {
        color: #7f8c8d;
        font-size: 1.2em;
        text-align: center;
        margin-bottom: 30px;
    }
    .card {
        background: white;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 20px;
    }
    .metric-card {
        background: linear-gradient(135deg, #2c353c, #3a454d); /* Dark gradient */
        color: "white" */
        border: 1px solid #2c353c; 
        border-radius: 15px;
        padding: 20px;
        margin: 10px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.2); 
    }
    .metric-value {
        font-size: 2.5em;
        font-weight: bold;
    }
    .metric-label {
        font-size: 1.2em;
        opacity: 0.9;
    }
            .stPyDeckChart {
            width: 100% !important;
        }
</style>
""", unsafe_allow_html=True)

# OSM highway classification colors (RGB)
osm_highway_colors = {
    "motorway": [255, 0, 0],          # Red (unchanged)
    "trunk": [255, 128, 0],           # Orange
    "primary": [255, 255, 0],         # Yellow
    "secondary": [128, 255, 0],       # Light green
    "tertiary": [200, 200, 200, 128], # Light gray with transparency (RGBA)
}

highway_widths = {
    "motorway": 8,
    "trunk": 7,
    "primary": 6,
    "secondary": 5,
    "tertiary": 4,
}

COLOR_BREWER_RED_SCALE = [
    [255, 245, 240],  # Lightest red (almost white)
    [254, 224, 210],  # Very light red
    [252, 187, 161],  # Light red
    [252, 146, 114],  # Medium-light red
    [251, 106, 74],   # Medium red
    [222, 45, 38],    # Dark red
    [165, 15, 21]     # Darkest red (optional, if you need a deeper red)
]

st.set_page_config(layout="wide")
st.title("Emergency database 🚨")

df_erupting_unrest = pd.read_csv("ETL/app/data/erupting_unrest.csv")
df_erupting = df_erupting_unrest[df_erupting_unrest['source']=='erupting']
df_unrest = df_erupting_unrest[df_erupting_unrest['source']=='unrest']
df_alert = pd.read_csv("ETL/app/data/alerts_volcanoes_latest.csv")
pop = gpd.read_file("ETL/app/data/population_at_risk.csv")

def parse_point(geom_str):
    coords = geom_str.replace("POINT (", "").replace(")", "").split()
    x, y = float(coords[0]), float(coords[1])
    return Point(x, y)

with open("ETL/app/data/osm_highways_features.pkl", "rb") as f:
    roads = pickle.load(f)
roads = pd.DataFrame(roads)

connectivity_node = gpd.read_file("ETL/app/data/all_nodes.gpkg")
all_emergency_services = gpd.read_file("ETL/app/data/all_emergency_services.gpkg")
all_essential_services = gpd.read_file("ETL/app/data/all_essential_services.gpkg")
all_amenities = gpd.read_file("ETL/app/data/all_amenities.gpkg")

volcanoes_erupting_list = df_erupting['Volcano_Name'].unique().tolist()
volcanoes_unrest_list = df_unrest['Volcano_Name'].unique().tolist()
volcanoes_list = volcanoes_erupting_list + volcanoes_unrest_list
volcanoes_list = sorted(volcanoes_list)

with (st.form("volcanoes")):
    col1 = st.columns(1)
    volcano_selected = st.selectbox(
                            "Select an erupting or unrest volcano in the database",
                            volcanoes_list,
                            index=9,
                            placeholder="Enter a volcano name",
                        )
    st.form_submit_button('Search')

df_volcano = df_erupting_unrest[df_erupting_unrest['Volcano_Name'] == volcano_selected]
df_alert_volcano = df_alert[df_alert['Name'] == volcano_selected]
geometry = [Point(xy) for xy in zip(df_volcano['Longitude'], df_volcano['Latitude'])]
gdf_volcano = gpd.GeoDataFrame(df_volcano, geometry=geometry, crs="EPSG:4326")
buffer_distance = 30000
gdf_volcano_projected = gdf_volcano.to_crs("EPSG:3857")
gdf_volcano_buffer = gdf_volcano_projected.geometry.buffer(buffer_distance)
gdf_volcano_buffer = gpd.GeoDataFrame(geometry=gdf_volcano_buffer, crs="EPSG:3857").to_crs("EPSG:4326")

df_pop = pop[pop['volcano_id'] == df_volcano.iloc[0,0]]
df_pop.loc[:, 'geometry'] = df_pop['geom'].apply(parse_point)
df_pop = gpd.GeoDataFrame(df_pop, geometry='geometry', crs="EPSG:4326")
pop_df = pd.DataFrame({
    'latitude': df_pop.geometry.y,
    'longitude': df_pop.geometry.x,
    'population': pd.to_numeric(df_pop['pop'], errors='coerce'),
})
df_roads = roads[roads['volcano_name'] == volcano_selected]
df_connectivity_node = connectivity_node[connectivity_node['id'] == df_volcano.iloc[0,0]]
df_connectivity_node['lng'] = df_connectivity_node.geometry.x
df_connectivity_node['lat'] = df_connectivity_node.geometry.y
df_emergency_services = all_emergency_services[all_emergency_services['id'] == df_volcano.iloc[0,0]]
df_emergency_services['lng'] = df_emergency_services.geometry.x
df_emergency_services['lat'] = df_emergency_services.geometry.y
df_essential_services = all_essential_services[all_essential_services['id'] == df_volcano.iloc[0,0]]
df_essential_services['lng'] = df_essential_services.geometry.x
df_essential_services['lat'] = df_essential_services.geometry.y
df_amenities = all_amenities[all_amenities['id'] == df_volcano.iloc[0,0]]
df_amenities['lng'] = df_amenities.geometry.x
df_amenities['lat'] = df_amenities.geometry.y

col_alert_1, col_alert_2 = st.columns([3, 3])

if df_alert_volcano is not None :
    with col_alert_1:
        st.markdown(
            f"""
            <div class="metric-card" style="
                background: linear-gradient(135deg, #2c353c, #3a454d); /* Dark gradient */
                border-radius: 12px;
                padding: 20px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.2);
                color: white;
                text-align: center;
                height: 120px;
                display: flex;
                flex-direction: column;
                justify-content: center;
                border-left: 4px solid #ff4500;
            ">
                <div class="metric-label" style="
                    font-size: 16px;
                    font-weight: 500;
                    margin-bottom: 8px;
                    opacity: 0.9;
                    color: black;
                ">Observatory Alert Level</div>
                <div class="metric-value" style="
                    font-size: 28px;
                    font-weight: 700;
                    letter-spacing: 1px;
                    color: white;
                ">{df_alert_volcano.iloc[0,1].capitalize()}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
else:
    st.warning("No Observatory Alert Level available.")

if df_alert_volcano is not None:

    with col_alert_2:
        st.markdown(
            f"""
            <div class="metric-card" style="
                background: linear-gradient(135deg, #2c353c, #3a454d); /* Dark gradient */
                border-radius: 12px;
                padding: 20px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.2);
                color: white;
                text-align: center;
                height: 120px;
                display: flex;
                flex-direction: column;
                justify-content: center;
                border-left: 4px solid #ff4500;
            ">
                <div class="metric-label" style="
                    font-size: 16px;
                    font-weight: 500;
                    margin-bottom: 8px;
                    opacity: 0.9;
                    color: black;
                ">Aviation Alert Level</div>
                <div class="metric-value" style="
                    font-size: 28px;
                    font-weight: 700;
                    letter-spacing: 1px;
                    color: white;
                ">{df_alert_volcano.iloc[0,2]}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

else:
    st.warning("No Aviation Alert Level available.")

st.markdown("")

left, right = st.columns([1, 2])

with left:
    for i, row in df_volcano.iterrows():
        st.markdown(f"**{row['Volcano_Name']}**")
        st.markdown(f"🌋 **Landform:** {row['Volcanic_Landform']}")
        st.markdown(f"🔥 **Type:** {row['Primary_Volcano_Type']}")
        st.markdown(f"⏳ **Last Eruption:** {int(row['Last_Eruption_Year']) or 'Unknown'}")
        st.markdown(f"🇺🇳 **Country:** {row['Country']}")
        st.markdown(f"📍 **Region:** {row['Region']}, {row['Subregion']}")
        st.markdown(f"📏 **Elevation:** {row['Elevation']} m")
        st.markdown(f"🌍 **Location:** {row['Latitude']}, {row['Longitude']}")

        # Display total population at risk
        total_pop = pop_df["population"].sum()
        st.markdown(
            f"""
             <div class="metric-card" style="background: linear-gradient(135deg,
                 {'#ff0000' if total_pop < 500000 else '#ff8c00'},
                 {'#ff0' if total_pop >= 500000 else '#ff4500'});">
                 <div class="metric-label">Total Population at Risk (30 km)</div>
                 <div class="metric-value">{total_pop:,}</div>
             </div>
             """,
            unsafe_allow_html=True,
        )

# Display images in middle column
with right:
    volcano_lat, volcano_lon = df_volcano['Latitude'].iloc[0], df_volcano['Longitude'].iloc[0]

    max_pop = pop_df['population'].max()
    pop_df['color'] = pop_df['population'].apply(
        lambda x: [255, int(255 - (x / max_pop * 255)), 0]  # Red → Yellow gradient
    )

    scatter_layer = pydeck.Layer(
        "ScatterplotLayer",
        data=pop_df,
        get_position=["longitude", "latitude"],
        get_radius=2,  # Scale radius by population
        get_fill_color="color",  # Use precomputed gradient
        pickable=True,
        radius_min_pixels=2,
        radius_max_pixels=20,
        stroked=False,
        line_width_min_pixels=1,
    )

    volcano_layer = pydeck.Layer(
        "ScatterplotLayer",
        data=[{"position": [volcano_lon, volcano_lat]}],
        get_position="position",
        get_radius=800,  # Fixed size
        get_fill_color=[255, 0, 0],  # Red marker
        pickable=True,
        stroked=True,
        get_line_color=[255, 255, 255],
        line_width_min_pixels=2,
    )

    view_state = pydeck.ViewState(
        latitude=volcano_lat,
        longitude=volcano_lon,
        zoom=9,
        bearing=0,
    )

    r = pydeck.Deck(
        layers=[scatter_layer, volcano_layer],
        initial_view_state=view_state,
        map_style=pydeck.map_styles.CARTO_DARK_NO_LABELS,
        tooltip={
            "text": "Population: {population}"
        },
    )

    st.pydeck_chart(r)

st.markdown("### 🗺️ Emergency map")

col_a, col_b = st.columns([3, 3])

if not df_roads.empty:

    with col_a:

        line_layer = pydeck.Layer(
            type="PathLayer",
            data=df_roads,
            pickable=False,
            get_color="color",
            width_scale=20,
            width_min_pixels=2,
            get_path="path",
            get_width="width",
        )

        r = pydeck.Deck(layers=[line_layer], initial_view_state=view_state,
                        map_style=pydeck.map_styles.CARTO_DARK_NO_LABELS,)

        st.pydeck_chart(r)

        legend_items = [
            f"""
            <div style="display: flex; align-items: center; margin: 5px 0;">
                <div style="
                    width: 10px;
                    height: {highway_widths.get(highway_type, 2)}px;
                    background-color: rgb({color[0]}, {color[1]}, {color[2]});
                    margin-right: 10px;
                "></div>
                <span>{highway_type}</span>
            </div>
            """
            for highway_type, color in osm_highway_colors.items()
        ]

        st.markdown(
            f"""
            <div style="margin-top: 15px; 
                        padding: 10px; 
                        background: linear-gradient(135deg, #2c353c, #3a454d); /* Dark gradient */
                        border-radius: 12px;
                        padding: 20px;
                        box-shadow: 0 4px 12px rgba(0,0,0,0.2);
                        border-left: 4px solid #ff4500;">
                <h4 style="margin-bottom: 10px;"> 🚗 OSM Highway Legend</h4>
                {''.join(legend_items)}
            </div>
            """,
            unsafe_allow_html=True
        )

else:
    st.warning("No road data available.")

if not df_connectivity_node.empty:

    with col_b:

        connectivity_layer = pydeck.Layer(
            type="HeatmapLayer",
            data=df_connectivity_node,
            opacity=0.9,
            get_position=["lng", "lat"],
            color_range=COLOR_BREWER_RED_SCALE,
            threshold=0.7,
            get_weight="score",
            pickable=True,
        )

        r = pydeck.Deck(layers=[connectivity_layer], initial_view_state=view_state,
                        map_style=pydeck.map_styles.CARTO_DARK_NO_LABELS,)

        st.pydeck_chart(r)

        st.markdown(
            """
            <div style="margin-top: 15px; 
                        padding: 10px; 
                        background: linear-gradient(135deg, #2c353c, #3a454d); /* Dark gradient */
                        border-radius: 12px;
                        padding: 20px;
                        box-shadow: 0 4px 12px rgba(0,0,0,0.2);
                        border-left: 4px solid #ff4500;">
                <h4 style="margin-bottom: 10px;"> 📍 Assembly points identification - Score Formula</h4>
                <p style="margin: 5px 0; font-family: monospace;">
                    <strong>score = (road_connectivity × α) + (density_population × β))</strong>
                </p>
                <p style="margin: 5px 0; font-size: 0.9em; color: #828282;">
                    <em>Where:</em><br>
                    • <strong>road_connectivity</strong> = Normalized betweenness centrality value of a road network node<br>
                    • <strong>density_population</strong> = Normalized population density value associated to a node<br>
                    • <strong>α, β</strong> = weighting coefficients
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

else:
    st.warning("No road data available.")

st.markdown("")

col_c, col_d, col_e = st.columns([3, 3, 3])

# Emergency Services (col_c) - Fixed Size
emergency_colors = {
    'fire_station': [255, 0, 0],       # Red
    'police': [0, 0, 255],            # Blue
    'hospital': [0, 255, 0],          # Green
    'ambulance_station': [255, 165, 0] # Orange
}

if not df_emergency_services.empty:
    df_emergency_services['color'] = df_emergency_services['amenity'].apply(
        lambda x: emergency_colors.get(x, [200, 200, 200])
    )

    with col_c:
        circle_emergency_layer = pydeck.Layer(
            "ScatterplotLayer",
            data=df_emergency_services,
            pickable=True,
            get_position=["lng", "lat"],
            get_radius=100,  # FIXED SIZE
            get_fill_color="color",
            radius_min_pixels=3,  # Same as get_radius
            radius_max_pixels=3,  # Same as get_radius
            stroked=False,
            get_line_color=[255, 255, 255],
            line_width_min_pixels=1,
        )

        buffer_layer = pydeck.Layer(
            "PolygonLayer",
            data=gdf_volcano_buffer,
            get_polygon="geometry.coordinates",
            get_fill_color=[211, 211, 211, 80],
            stroked=True,
            line_width_min_pixels=2,
            get_line_color=[242, 242, 242],
        )

        r = pydeck.Deck(
            layers=[volcano_layer, buffer_layer, circle_emergency_layer],
            initial_view_state=view_state,
            map_style=pydeck.map_styles.CARTO_DARK,
            tooltip={
                "text": "{amenity}"
            },
        )
        st.pydeck_chart(r)

        legend_items = [
            f"""
            <div style="display: flex; align-items: center; margin: 8px 0;">
                <div style="
                    color: #ffffff;
                    width: 16px;
                    height: 16px;
                    background-color: rgb({emergency_colors[service][0]}, {emergency_colors[service][1]}, {emergency_colors[service][2]});
                    border-radius: 50%;
                    margin-right: 10px;
                    border: 1px solid #666;
                "></div>
                <span style="color: #ffffff;">{service.replace('_', ' ').title()}</span>
            </div>
            """
            for service in emergency_colors.keys()
        ]

        st.markdown(
            f"""
            <div style="margin-top: 15px; 
                        padding: 10px; 
                        background: linear-gradient(135deg, #2c353c, #3a454d); /* Dark gradient */
                        border-radius: 12px;
                        padding: 20px;
                        box-shadow: 0 4px 12px rgba(0,0,0,0.2);
                        border-left: 4px solid #ff4500;">
                <h4 style="margin-bottom: 10px; color: #ffffff;">⚠️ Emergency Services</h4>
                {''.join(legend_items)}
            </div>
            """,
            unsafe_allow_html=True
        )
else:
    with col_c:
        st.warning("⚠️ The OSM server returned no emergency services")


essential_colors = {
    'supermarket': [75, 0, 130],        # Indigo
    'fuel': [255, 140, 0],             # Dark Orange
    'chemist': [147, 112, 219],        # Medium Purple
    'pharmacy': [147, 112, 219],       # Same as chemist
    'bank': [0, 191, 255],             # Deep Sky Blue
    'shelter': [128, 0, 0],            # Maroon
    'dentist': [0, 255, 255],          # Cyan
    'doctors': [30, 144, 255],         # Dodger Blue
    'embassy': [218, 112, 214],        # Orchid
    'townhall': [192, 192, 192],       # Silver
    'courthouse': [138, 43, 226],     # Blue Violet
    'veterinary': [34, 139, 34]        # Forest Green
}

if not df_essential_services.empty:
    df_essential_services['color'] = df_essential_services['amenity'].apply(
        lambda x: essential_colors.get(x, [200, 200, 200])
    )

    with col_d:
        circle_layer = pydeck.Layer(
            "ScatterplotLayer",
            data=df_essential_services,
            pickable=True,
            get_position=["lng", "lat"],
            get_radius=10,
            get_fill_color="color",
            radius_min_pixels=3,
            radius_max_pixels=3,
            stroked=False,
            get_line_color=[255, 255, 255],
            line_width_min_pixels=1,
        )

        r = pydeck.Deck(
            layers=[volcano_layer, buffer_layer, circle_layer],
            initial_view_state=view_state,
            map_style=pydeck.map_styles.CARTO_DARK,
            tooltip={
                "text": "{amenity}"
            },
        )
        st.pydeck_chart(r)

        legend_essential_items = [
            f"""
            <div style="display: flex; align-items: center; margin: 8px 0;">
                <div style="
                    width: 16px;
                    height: 16px;
                    background-color: rgb({essential_colors[service][0]}, {essential_colors[service][1]}, {essential_colors[service][2]});
                    border-radius: 50%;
                    margin-right: 10px;
                    border: 1px solid #666;
                "></div>
                <span style="color: ffffff;">{service.replace('_', ' ').title()}</span>
            </div>
            """
            for service in essential_colors.keys()
        ]

        st.markdown(
            f"""
            <div style="margin-top: 15px; 
                        padding: 10px; 
                        background: linear-gradient(135deg, #2c353c, #3a454d); /* Dark gradient */
                        border-radius: 12px;
                        padding: 20px;
                        box-shadow: 0 4px 12px rgba(0,0,0,0.2);
                        border-left: 4px solid #ff4500;">
                <h4 style="margin-bottom: 10px; color: #ffffff;">🏪 Essential Services</h4>
                {''.join(legend_essential_items)}
            </div>
            """,
            unsafe_allow_html=True
        )
else:
    with col_d:
        st.warning("⚠️ The OSM server returned no essential services")

amenity_colors = {
    'kindergarten': [255, 182, 193],   # Light Pink
    'school': [255, 218, 185],        # Peach Puff
    'library': [173, 216, 230],       # Light Blue
    'college': [144, 238, 144],       # Light Green
    'university': [30, 144, 255],     # Dodger Blue
    'social_facility': [255, 192, 203], # Pink
    'prison': [100, 100, 100],  # Gray
    'nursing_home': [255, 215, 0],  # Gold
}

if not df_amenities.empty:
    df_amenities['color'] = df_amenities['amenity'].apply(
        lambda x: amenity_colors.get(x, [200, 200, 200])
    )

    with col_e:
        circle_amenities_layer = pydeck.Layer(
            "ScatterplotLayer",
            data=df_amenities,
            pickable=True,
            get_position=["lng", "lat"],
            get_radius=10,
            get_fill_color="color",
            radius_min_pixels=3,
            radius_max_pixels=3,
            stroked=False,
            get_line_color=[255, 255, 255],
            line_width_min_pixels=1,
        )

        r = pydeck.Deck(
            layers=[volcano_layer, buffer_layer, circle_amenities_layer],
            initial_view_state=view_state,
            map_style=pydeck.map_styles.CARTO_DARK,
            tooltip={
                "text": "{amenity}"
            },
        )
        st.pydeck_chart(r)

        legend_amenities_items = [
            f"""
            <div style="display: flex; align-items: center; margin: 8px 0;">
                <div style="
                    width: 16px;
                    height: 16px;
                    background-color: rgb({amenity_colors[service][0]}, {amenity_colors[service][1]}, {amenity_colors[service][2]});
                    border-radius: 50%;
                    margin-right: 10px;
                    border: 1px solid #666;
                "></div>
                <span style="color: ffffff;">{service.replace('_', ' ').title()}</span>
            </div>
            """
            for service in amenity_colors.keys()
        ]

        st.markdown(
            f"""
            <div style="margin-top: 15px; 
                        padding: 10px; 
                        background: linear-gradient(135deg, #2c353c, #3a454d); /* Dark gradient */
                        border-radius: 12px;
                        padding: 20px;
                        box-shadow: 0 4px 12px rgba(0,0,0,0.2);
                        border-left: 4px solid #ff4500;">
                <h4 style="margin-bottom: 10px; color: #ffffff;">🎓 Educational Facilities and others</h4>
                {''.join(legend_amenities_items)}
            </div>
            """,
            unsafe_allow_html=True
        )
else:
    with col_e:
        st.warning("⚠️ The OSM server returned no amenity")


st.markdown("---")  # Add a horizontal line separator
st.markdown("### 📚 Data Sources")

# First source
st.markdown("""
**Global mosaic 2025 (1km resolution) R2025 v1:**  
Bondarenko M., Priyatikanto R., Tejedor-Garavito N., Zhang W., McKeen T., Cunningham A., Woods T., Hilton J., Cihan D., Nosatiuk B., Brinkhoff T., Tatem A., Sorichetta A.. 2025.   
The spatial distribution of population in 2015-2030 at a resolution of 30 arc (approximately 1km at the equator) R2025A version v1. Global Demographic Data Project  
Funded by The Bill and Melinda Gates Foundation (INV-045237)  
WorldPop - School of Geography and Environmental Science, University of Southampton   
[DOI](10.5258/SOTON/WP00845)  
""")

# Second source
st.markdown("""
**OSM DATA:**
OpenStreetMap contributors. (2017). Planet dump retrieved from https://planet.osm.org  
[OSM](https://www.openstreetmap.org)  
""")

st.markdown("""
**Global Volcanism Program:**  
Global Volcanism Program (2025).  
*Volcanoes of the World (v. 5.3.2; 30 Sep 2025)*.  
Distributed by Smithsonian Institution, compiled by Venzke, E.  
[DOI: 10.5479/si.GVP.VOTW5-2025.5.3](https://doi.org/10.5479/si.GVP.VOTW5-2025.5.3)  
""")

# Optional note
st.caption("This application uses data from these sources under their respective licenses. Please cite the original sources when using this data for research purposes.")


st.markdown("""
<div style="text-align: center; margin-top: 30px; color: #7f8c8d; font-size: 0.9em;">
    © 2023 Olivier Gillet | Volcanic Data Pipeline Project
</div>
""", unsafe_allow_html=True)
