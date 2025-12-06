import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(
    page_title="Informations about Holocene volcanoes",
    page_icon="𝄜",
    layout="wide",
)

st.title("🌋 Holocene volcanoes database")

# Custom CSS
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
        border: 1px solid #FFD166; 
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
</style>
""", unsafe_allow_html=True)


df_volcanoes = pd.read_csv("ETL/app/data/volcanoes_db.csv")
df_volcanoes = df_volcanoes.rename(columns={"x_coordinate": "longitude", "y_coordinate": "latitude"})
volcanoes_list = sorted(df_volcanoes['Volcano_Name'].unique().tolist())

df_historical_eruptions = pd.read_csv("ETL/app/data/historical_db.csv")
df_historical_eruptions = df_historical_eruptions.rename(columns={"x_coordinate": "longitude", "y_coordinate": "latitude"})

df_historical_eruptions_GVP = pd.read_csv("ETL/app/data/historical_db_GVP.csv")
df_historical_eruptions_GVP = df_historical_eruptions_GVP.rename(columns={"x_coordinate": "longitude", "y_coordinate": "latitude"})

with (st.form("volcanoes")):
    col1 = st.columns(1)
    volcano_selected = st.selectbox(
                            "Select a volcano",
                            volcanoes_list,
                            index=1,
                            placeholder="Enter a volcano name",
                        )
    st.form_submit_button('Search')

df_volcano = df_volcanoes[df_volcanoes['Volcano_Name'] == volcano_selected]
df_historical_eruptions_GVP_volcano = df_historical_eruptions_GVP[df_historical_eruptions_GVP['Volcano_Name'] == volcano_selected]
df_historical_eruptions_volcano = df_historical_eruptions[df_historical_eruptions['Volcano Name'] == volcano_selected]
print(df_historical_eruptions_volcano)

left, right = st.columns([1, 2])

# Display volcano names in left column
with left:
    for i, row in df_volcano.iterrows():
        st.markdown(f"**{row['Volcano_Name']}**")
        st.markdown(f"🌋 **Landform:** {row['Volcanic_Landform']}")
        st.markdown(f"🔥 **Type:** {row['Primary_Volcano_Type']}")
        last_eruption = (
            int(row['Last_Eruption_Year'])
            if pd.notna(row['Last_Eruption_Year']) and row['Last_Eruption_Year'] != ''
            else "Unknown"
        )
        st.markdown(f"⏳ **Last Eruption:** {last_eruption}")
        st.markdown(f"🇺🇳 **Country:** {row['Country']}")
        st.markdown(f"📍 **Region:** {row['Region']}, {row['Subregion']}")
        st.markdown(f"📏 **Elevation:** {row['Elevation']} m")
        st.markdown(f"🌍 **Location:** {row['Latitude']}, {row['Longitude']}")

        # Add expandable section for geological summary
        with st.expander("📖 Geological Summary", expanded=True):
            st.markdown(row['Geological_Summary'])

# Display images in middle column
with right:
    for i, row in df_volcano.iterrows():
        st.image(
            row['Primary_Photo_Link'],
            caption=row['Primary_Photo_Caption'],
            use_container_width=True
        )
        st.markdown(f"©️ **credit:** {row['Primary_Photo_Credit']}")

if not df_historical_eruptions_GVP.empty:
    # Summary statistics
    st.markdown("### 📊 Data Summary")

    col_a, col_b, col_c = st.columns([2, 2, 2])
    with col_a:
        st.markdown(f"""
           <div class="metric-card">
               <div class="metric-label">Total Records (GVP database) </div>
               <div class="metric-value">{len(df_historical_eruptions_GVP_volcano):,}</div>
           </div>
           """, unsafe_allow_html=True)

    with col_b:
        if df_historical_eruptions_volcano.empty:
            total_records = "0"
        else:
            try:
                total_records = f"{len(df_historical_eruptions_volcano)}"  # Format with thousands separator
            except (ValueError, TypeError):
                total_records = "0"

        st.markdown(f"""
           <div class="metric-card">
               <div class="metric-label">Total Records (Volcanic fatalities db) </div>
               <div class="metric-value">{total_records}</div>
           </div>
           """, unsafe_allow_html=True)

    with col_c:
        if df_historical_eruptions_volcano.empty:
            fatalities_display = "0"
        else:
            try:
                fatalities_display = f"{df_historical_eruptions_volcano.iloc[0]['Number of fatalities']}"  # Format with thousands separator
            except (ValueError, TypeError):
                fatalities_display = "0"

        # Then use in your HTML
        st.markdown(f"""
           <div class="metric-card">
               <div class="metric-label">Total Records (Volcanic fatalities db) </div>
               <div class="metric-value">{fatalities_display}</div>
           </div>
           """, unsafe_allow_html=True)

    # Visualizations
    st.markdown("### 📈 Data Visualizations")

    tab1, tab2 = st.tabs(["Time Series", ""])

    if not df_historical_eruptions_GVP_volcano.empty:
        with tab1:

            eruptions_recent = df_historical_eruptions_GVP_volcano[
                df_historical_eruptions_GVP_volcano['StartDateYear'] >= 1950
                ].copy()

            eruptions_recent['Decade'] = (eruptions_recent['StartDateYear'] // 10) * 10
            eruptions_by_decade = eruptions_recent.groupby('Decade').size().reset_index(name='eruption_count')

            # Sort by decade
            eruptions_by_decade = eruptions_by_decade.sort_values('Decade')

            # Create the time series bar plot
            fig = px.bar(eruptions_by_decade,
                         x='Decade',
                         y='eruption_count',
                         title='Number of Eruptions per Year',
                         labels={
                             'eruption_count': 'Number of Eruptions',
                             'StartDateYear': 'Year'  # Updated to match your column
                         },
                         color='eruption_count',
                         color_continuous_scale='Oranges')

            # Update layout for better appearance
            fig.update_layout(
                xaxis_title="Year",
                yaxis_title="Number of Eruptions",
                hovermode="x unified",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                height=500
            )

            # Show the plot
            st.plotly_chart(fig, use_container_width=True)

            # Optional: Show data summary
            with st.expander("View data summary"):
                st.write(f"Total eruptions: {eruptions_by_decade['eruption_count'].sum()}")
                st.write(f"Decade with eruptions: {len(eruptions_by_decade)}")
    else:
        with tab1:
            st.warning("⚠️ No eruption data available for this volcano")

st.markdown("---")  # Add a horizontal line separator
st.markdown("### 📚 Data Sources")

# First source
st.markdown("""
**Volcanic Fatalities Database:**
Brown, Sarah; Jenkins, Susanna; Sparks, R.; Odbert, Henry; Auker, Melanie (2017).
*Additional file 1: of Volcanic fatalities database: analysis of volcanic threat with distance and victim classification*.
[figshare Dataset](https://doi.org/10.6084/m9.figshare.c.3885196_D1.v1)
""")

# Second source
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