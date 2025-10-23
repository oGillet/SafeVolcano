import pydeck
import pandas as pd
import streamlit as st


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
        background: linear-gradient(135deg, #6e8efb, #a777e3);
        color: white;
        border-radius: 10px;
        padding: 20px;
        margin: 10px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
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

st.set_page_config(layout="wide")
st.title("Interactive Map: Active Volcanoes 🌋")

# Load datasets
df_erupting_unrest = pd.read_csv("ETL/app/data/erupting_unrest_volcanoes_latest.csv")
df_erupting = df_erupting_unrest[df_erupting_unrest['source']=='erupting']
df_unrest = df_erupting_unrest[df_erupting_unrest['source']=='unrest']
df_total_affected = pd.read_csv("ETL/app/data/total_affected.csv")
df_historical_db = pd.read_csv("ETL/app/data/historical_db.csv")
df_volcanoes = pd.read_csv("ETL/app/data/volcanoes_db.csv")
region = df_volcanoes['Region'].unique()

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
    col1, col2 = st.columns(2)

    # Region selector
    region_selected = col1.selectbox('Select a region', regions_list, index=0)

    # Layer selector
    layer_options = {
        "Show both": ["volcano", "volcanoes_erupting", "volcanoes_unrest"],
        "Show only erupting & unrest volcanoes": ["volcanoes_erupting", "volcanoes_unrest"],
        "Show all holocene volcanoes": ["volcano"]
    }
    layer_selected = col2.selectbox('Select layers to display', list(layer_options.keys()), index=1)

    st.form_submit_button('Update map')

# Filter data based on selected region
if region_selected == "All Regions":
    filtered_volcanoes_erupting = df_erupting
    filtered_volcanoes_unrest = df_unrest
    filtered_volcanoes = df_volcanoes
else:
    filtered_volcanoes_erupting = df_erupting[df_erupting['Region'] == region_selected]
    filtered_volcanoes_unrest = df_unrest[df_erupting['Region'] == region_selected]
    filtered_volcanoes = df_volcanoes[df_volcanoes['Region'] == region_selected]


# Create icon layer for erupting volcanoes
db_volcanoes_erupting = pydeck.Layer(
    "ScatterplotLayer",
    data=filtered_volcanoes_erupting,
    id="volcanoes_erupting",
    get_position=["Longitude", "Latitude"],
    get_color="[250,0,0]",
    pickable=True,
    auto_highlight=True,
    get_radius=100000,
)

db_volcanoes_unrest = pydeck.Layer(
    "ScatterplotLayer",
    data=filtered_volcanoes_unrest,
    id="volcanoes_erupting",
    get_position=["Longitude", "Latitude"],
    get_color="[255,165,0]",
    pickable=True,
    auto_highlight=True,
    get_radius=100000,
)

# Create scatterplot layer for regular volcanoes
db_volcanoes = pydeck.Layer(
    "ScatterplotLayer",
    data=filtered_volcanoes,
    id="volcano",
    get_position=["Longitude", "Latitude"],
    get_color="[169, 169, 169, 60]",
    pickable=True,
    auto_highlight=True,
    get_radius=100000,
)


# Determine which layers to show based on selection
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

if region_selected != "All Regions":
    if selected_layer_ids == ["volcano"]:
        center_lat = filtered_volcanoes['Latitude'].mean()
        center_lon = filtered_volcanoes['Longitude'].mean()
    if selected_layer_ids == ["volcanoes_erupting", "volcanoes_unrest"]:
        if filtered_volcanoes_erupting.empty or filtered_volcanoes_unrest.empty:
            print("No volcanoes found for the selected region.")
            center_lat = 0
            center_lon = 0
        else:
            print(f"Found {len(filtered_volcanoes_erupting)+len(filtered_volcanoes_unrest)} volcanoes for the selected region.")
            center_lat = filtered_volcanoes_erupting['Latitude'].mean()
            center_lon = filtered_volcanoes_erupting['Longitude'].mean()
    if selected_layer_ids == ["volcano","volcanoes_erupting", "volcanoes_unrest"]:
        center_lat = filtered_volcanoes['Latitude'].mean()
        center_lon = filtered_volcanoes['Longitude'].mean()
else:
    center_lat = 0
    center_lon = 0

view_state = pydeck.ViewState(
    latitude=center_lat,
    longitude=center_lon,
    zoom=1.5 if region_selected == all_regions_option else 4,  # Wider zoom for all regions
)

chart = pydeck.Deck(
    layers=layers_to_show,
    initial_view_state=view_state,
    map_style=pydeck.map_styles.CARTO_DARK_NO_LABELS,
    tooltip={"text": "🌋 {Volcano_Name}"},
)

# Display the chart with increased size
st.pydeck_chart(chart, use_container_width=True, height=800)

if not df_erupting.empty:
    # Summary statistics
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### 📊 🌎 Data Summary")

    col_a, col_b, col_c = st.columns([2, 2, 2])
    with col_a:
        st.markdown(f"""
           <div class="metric-card">
               <div class="metric-label">Active volcanoes<br> </div>
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