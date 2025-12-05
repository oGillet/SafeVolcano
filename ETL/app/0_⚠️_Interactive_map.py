import pydeck
import pandas as pd
import streamlit as st
import base64

st.markdown("""
<style>
    .stButton>button {
        background-color: #ffffff;
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
        background-color: #ffffff;
    }
    .stSelectbox, .stMultiSelect, .stSlider {
        margin-bottom: 20px;
    }
    .header {
        color: #ffffff;
        font-size: 2.5em;
        text-align: center;
        margin-bottom: 10px;
    }
    .subtitle {
        color: #ffffff;
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
        border: 1px solid #FFD166; 
        border-radius: 15px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.2);
    }
</style>
""", unsafe_allow_html=True)

st.set_page_config(layout="wide")
st.title("TEST - Active Volcanoes 🌋 and recent eartquakes ⚠️")

def depth_to_color(depth):
    # Shallow = yellow, deep = red
    return [255, max(0, 255 - int(depth * 10)), 0]

# Load datasets
df_erupting_unrest = pd.read_csv("ETL/app/data/erupting_unrest.csv")
date_volcanoes = df_erupting_unrest['date'].unique()
df_erupting = df_erupting_unrest[df_erupting_unrest['source']=='erupting']
df_unrest = df_erupting_unrest[df_erupting_unrest['source']=='unrest']
df_total_affected = pd.read_csv("ETL/app/data/total_affected.csv")
df_historical_db = pd.read_csv("ETL/app/data/historical_db.csv")
df_volcanoes = pd.read_csv("ETL/app/data/volcanoes_db.csv")
df_earthquakes = pd.read_csv("ETL/app/data/earthquakes_db.csv")
df_earthquakes["color"] = df_earthquakes["depth"].apply(depth_to_color)
df_earthquakes["radius"] = df_earthquakes["magnitude"] * 60000
date_earthquakes = df_earthquakes['date'].unique()

# Display in Streamlit
st.markdown("---")
st.markdown("##### 📅 Database Information")
st.caption(f"""
<div>
    <p><strong>🌍 Earthquake Database</strong> - Last updated: <strong>{pd.to_datetime(max(df_earthquakes['date'])).strftime('%d %B %Y')}</strong> (USGS Earthquake Hazards Program) </p>
</div>
<div>
    <p><strong>🌋 Volcano Database</strong> - Last updated: <strong>{pd.to_datetime(max(df_erupting_unrest['date'])).strftime('%d %B %Y')}</strong> (Smithsonian / USGS Daily Volcanic Activity Report) </p>
</div>
""", unsafe_allow_html=True)


region = df_erupting_unrest['Region'].unique()

# Drop duplicates in df_historical_db (keep first occurrence)
df_historical_db = df_historical_db.drop_duplicates(subset=["Volcano Name"])

# Now merge
df_erupting = pd.merge(
    df_erupting,
    df_historical_db[["Volcano Name", "Population VPI5", "Population VPI 10", "Population VPI30", "Population VPI100"]],
    left_on="Volcano_Name",
    right_on="Volcano Name",
    how="left"
)

df_unrest = pd.merge(
    df_unrest,
    df_historical_db[["Volcano Name", "Population VPI5", "Population VPI 10", "Population VPI30", "Population VPI100"]],
    left_on="Volcano_Name",
    right_on="Volcano Name",
    how="left"
)

df_volcanoes = pd.merge(
    df_volcanoes,
    df_historical_db[["Volcano Name", "Population VPI5", "Population VPI 10", "Population VPI30", "Population VPI100"]],
    left_on="Volcano_Name",
    right_on="Volcano Name",
    how="left"
)

st.set_page_config(
    page_title="Active Volcanoes",
    page_icon="️️🚨",
)

# Create an option for "All Regions" and prepend it to the region list
all_regions_option = "All Regions"
regions_list = [all_regions_option] + list(region)

with st.form("map"):
    # Layer selector
    layer_options = {
        "Show erupting, unrest and dormant": ["volcano", "volcanoes_erupting", "volcanoes_unrest"],
        "Show only erupting & unrest volcanoes": ["volcanoes_erupting", "volcanoes_unrest"],
        "Show all holocene volcanoes": ["volcano"],
        "Show only recent earthquakes": ["earthquake"],
        "Show all layers": ["all_layers"]
    }
    layer_selected = st.selectbox('Select layers to display', list(layer_options.keys()), index=4)

    st.form_submit_button('Update map')

filtered_volcanoes_erupting = df_erupting
filtered_volcanoes_unrest = df_unrest
filtered_volcanoes = df_volcanoes
filtered_earthquakes = df_earthquakes

with open("ETL/app/data/images/volcano_red.png", "rb") as f:
    b64 = base64.b64encode(f.read()).decode()
volcano_icon_url = f"data:image/png;base64,{b64}"
filtered_volcanoes_erupting["icon_data"] = [{
    "url": volcano_icon_url,
    "width": 128,
    "height": 128,
    "anchorY": 128,
    "anchorX": 64
} for _ in range(len(filtered_volcanoes_erupting))]
filtered_volcanoes_erupting["tooltip_html"] = (
    "<b>🌋:</b> " + filtered_volcanoes_erupting["Volcano_Name"] +
    "<br><b>Status:</b> " + "erupting"
)

with open("ETL/app/data/images/volcano_orange.png", "rb") as f:
    b64 = base64.b64encode(f.read()).decode()
volcano_icon_url = f"data:image/png;base64,{b64}"
filtered_volcanoes_unrest["icon_data"] = [{
    "url": volcano_icon_url,
    "width": 128,
    "height": 128,
    "anchorY": 128,
    "anchorX": 64
} for _ in range(len(filtered_volcanoes_unrest))]
filtered_volcanoes_unrest["tooltip_html"] = (
    "<b>🌋:</b> " + filtered_volcanoes_unrest["Volcano_Name"] +
    "<br><b>Status:</b> " + "unrest"
)

with open("ETL/app/data/images/volcano_white.png", "rb") as f:
    b64 = base64.b64encode(f.read()).decode()
volcano_icon_url = f"data:image/png;base64,{b64}"
filtered_volcanoes["icon_data"] = [{
    "url": volcano_icon_url,
    "width": 128,
    "height": 128,
    "anchorY": 128,
    "anchorX": 64
} for _ in range(len(filtered_volcanoes))]
filtered_volcanoes["tooltip_html"] = (
    "<b>🌋:</b> " + filtered_volcanoes["Volcano_Name"] +
    "<br><b>Status:</b> " + "dormant"
)

def make_icon(df, color):
    return pydeck.Layer(
        "IconLayer",
        data=df,
        pickable=True,
        auto_highlight=True,
        get_icon="icon_data",
        get_position=["Longitude", "Latitude"],
        size_scale=12,
        get_size=1,
    )

RED = [255, 0, 0, 255]
ORANGE = [255, 140, 0, 255]
WHITE = [255, 255, 255, 255]

db_volcanoes_erupting = make_icon(df_erupting, RED)
db_volcanoes_unrest = make_icon(df_unrest, ORANGE)
db_volcanoes = make_icon(filtered_volcanoes, WHITE)

filtered_earthquakes["tooltip_html"] = (
    "<b>ﮩ٨ـﮩﮩ٨ـ </b> " + filtered_earthquakes["magnitude"].round(1).astype(str) +
    "<br><b>Depth:</b> " + filtered_earthquakes["depth"].round(1).astype(str)
)

db_earthquakes = pydeck.Layer(
    "ScatterplotLayer",
    data=filtered_earthquakes,
    id="earthquakes",
    pickable=True,
    auto_highlight=True,
    opacity=0.1,
    get_position=["x_coordinate", "y_coordinate"],
    get_color="color",
    get_radius="radius",
    radius_min_pixels=3,
    radius_max_pixels=50,
)


layers_to_show = []
selected_layer_ids = layer_options[layer_selected]

if selected_layer_ids == ["volcano"]:
    layers_to_show.append(db_volcanoes)
if selected_layer_ids == ["volcanoes_erupting", "volcanoes_unrest"]:
    layers_to_show.append(db_volcanoes_erupting)
    layers_to_show.append(db_volcanoes_unrest)
if selected_layer_ids == ["volcano","volcanoes_erupting", "volcanoes_unrest"]:
    layers_to_show.append(db_volcanoes)
    layers_to_show.append(db_volcanoes_erupting)
    layers_to_show.append(db_volcanoes_unrest)
if selected_layer_ids == ["earthquake"]:
    layers_to_show.append(db_earthquakes)
if selected_layer_ids == ["all_layers"]:
    layers_to_show.append(db_volcanoes)
    layers_to_show.append(db_volcanoes_erupting)
    layers_to_show.append(db_volcanoes_unrest)
    layers_to_show.append(db_earthquakes)

center_lat = 0
center_lon = 0

view_state = pydeck.ViewState(
    latitude=center_lat,
    longitude=center_lon,
    zoom=1.5,
)

chart = pydeck.Deck(
    layers=layers_to_show,
    initial_view_state=view_state,
    map_style=pydeck.map_styles.CARTO_DARK_NO_LABELS,
    tooltip = {"html": "{tooltip_html}"}
)

# Display the chart with increased size
st.pydeck_chart(chart, use_container_width=True, height=800)

if not df_erupting.empty:
    # Summary statistics
    st.markdown("### 📊 🌎 Data Summary")

    col_a, col_b, col_c = st.columns([2, 2, 2])
    with col_a:
        st.markdown(f"""
           <div class="metric-card">
               <div class="metric-label">Active volcanoes ({pd.to_datetime(max(df_erupting_unrest['date'])).strftime('%d %B %Y')}) <br> </div>
               <div class="metric-value">{len(df_erupting)+len(df_unrest):,}</div>
           </div>
           """, unsafe_allow_html=True)

    with col_b:
        st.markdown(f"""
           <div class="metric-card">
               <div class="metric-label">Erupting<br> </div>
               <div class="metric-value">{len(df_erupting)}</div>
           </div>
           """, unsafe_allow_html=True)

    with col_c:
        st.markdown(f"""
           <div class="metric-card">
               <div class="metric-label">Unrest<br> </div>
               <div class="metric-value">{len(df_unrest)}</div>
           </div>
           """, unsafe_allow_html=True)

    col_d, col_e, col_f = st.columns([2, 2, 2])
    with col_d:
        st.markdown(f"""
           <div class="metric-card">
               <div class="metric-label">Earthquakes ({pd.to_datetime(max(df_earthquakes['date'])).strftime('%d %B %Y')}) <br> </div>
               <div class="metric-value">{len(df_earthquakes):,}</div>
           </div>
           """, unsafe_allow_html=True)

    with col_e:
        st.markdown(f"""
           <div class="metric-card">
               <div class="metric-label">Magnitude (maximum observed) <br> </div>
               <div class="metric-value">{max(df_earthquakes['magnitude']):,}</div>
           </div>
           """, unsafe_allow_html=True)

    with col_f:
        st.markdown(f"""
           <div class="metric-card">
               <div class="metric-label">Depth (mean observed) <br> </div>
               <div class="metric-value">{df_earthquakes['depth'].mean().round(1):,}</div>
           </div>
           """, unsafe_allow_html=True)

st.markdown("---")  # Add a horizontal line separator
st.markdown("### 📚 Data Sources")

# First source
st.markdown("""
**Global Volcano Database:**
[Smithsonian Institution Global Volcanism Program](https://volcano.si.edu)
- Smithsonian / USGS Daily Volcanic Activity Report
""")

# Second source
st.markdown("""
**Real-time Earthquake Data:**
[USGS Earthquake Hazards Program](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php)
- API endpoint: `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson`
""")

# Optional note
st.caption("This application uses data from these sources under their respective licenses. Please cite the original sources when using this data for research purposes.")

st.markdown("""
<div style="text-align: center; margin-top: 30px; color: #7f8c8d; font-size: 0.9em;">
    © 2023 Olivier Gillet | Volcanic Data Pipeline Project
</div>
""", unsafe_allow_html=True)