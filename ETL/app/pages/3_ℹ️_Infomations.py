import streamlit as st

st.set_page_config(
    page_title="Volcanic Data Pipeline",
    page_icon="🌋",
    layout="wide"
)

# Custom CSS for styling
st.markdown("""
<style>
    .main {
        background-color: #f8f9fa;
    }
    .stMarkdown {
        font-size: 1.1em;
    }
    .section-header {
        color: #d63031;
        border-bottom: 2px solid #dfe6e9;
        padding-bottom: 5px;
        margin: 5px;
    }
    .tech-stack {
        background: linear-gradient(135deg, #2c353c, #3a454d); /* Dark gradient */
        padding: 15px;
        border-radius: 5px;
    }
    .component {
        background: linear-gradient(135deg, #2c353c, #3a454d); /* Dark gradient */
        padding: 15px;
        border-radius: 5px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 20px;
    }
    .icon {
        font-size: 1.5em;
        margin-right: 10px;
    }
    
    .author-card {
        background-color: #2c353c;
        padding: 20px;
        border-radius: 5px;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        text-align: center;
        border: 1px solid #eaeaea;
    }
    .author-name {
        color: "white";
        font-size: 1.5em;
        margin-bottom: 5px;
    }
    .author-title {
        color: #7f8c8d;
        font-style: italic;
        margin-bottom: 15px;
    }
    .contact-info {
        margin-top: 15px;
        padding-top: 15px;
        border-top: 1px solid #eaeaea;
    }
    .email {
        color: #3498db;
        text-decoration: none;
    }
    .email:hover {
        text-decoration: underline;
    }
</style>
""", unsafe_allow_html=True)

# Header with title and description
st.title("🌋 Volcanic Data Pipeline")
st.markdown("""
<div style='text-align: justify;'>
My first <strong>ETL (Extract, Transform, Load)</strong> pipeline designed to collect, process, and visualize data on erupting and unrest volcanoes from the <strong>Smithsonian Institution's Global Volcanism Program (GVP)</strong>.
</div>
""", unsafe_allow_html=True)


# Components Section
st.markdown('<h4 class="section-header">⚙️ Key Components</h4>', unsafe_allow_html=True)

# Data Extraction
st.markdown("""
<div class="component">
<span class="icon">📥</span>
<strong>Data Extraction</strong>
<ul>
    <li>Automated collection of structured data from <a href="https://volcano.si.edu/" target="_blank">Smithsonian's GVP</a> (web scraping)</li>
    <li>Key datasets:
        <ul>
            <li><strong>Smithsonian/USGS Volcanic Activity:</strong> Daily updates on volcanic eruptions and unrest periods</li>
            <li><strong>Geospatial Coordinates:</strong> WGS84 latitude/longitude pairs for volcano location mapping</li>
            <li><strong>Holocene Eruption Catalog:</strong> Chronological database of volcanic events with VEI classifications</li>
            <li><strong>Geological Metadata:</strong> Volcano morphology, etc.</li>
            <li><strong>Seismological Data:</strong> Earthquake events (magnitude, depth, location) from USGS</li>
        </ul>
    </li>
</ul>
</div>
""", unsafe_allow_html=True)

# Transformation
st.markdown("""
<div class="component">
<span class="icon">🔧</span>
<strong>Transformation & Enrichment</strong>
<ul>
    <li><strong>Data Cleaning & Data Enrichment:</strong>
        <ul>
            <li>Handling missing values</li>
            <li>Standardizing date formats</li>
            <li>Removing duplicate records</li>
            <li>Joining geological metadata</li>
            <li>Adding population exposure data</li>
        </ul>
    </li>
</ul>
</div>
""", unsafe_allow_html=True)

# Orchestration and Storage in two columns
col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    <div class="component">
    <span class="icon">⏰</span>
    <strong>Orchestration (with Airflow)</strong>
    <ul>
        <li>Daily scheduled updates</li>
        <li>Dependency management between tasks</li>
        <li>Automatic retries on failure</li>
        <li>Monitoring and alerting</li>
    </ul>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div class="component">
    <span class="icon">🗃️</span>
    <strong>Storage (PostgreSQL)</strong>
    <ul>
        <li>Relational database design</li>
        <li>Key tables:
            <ul>
                <li>Erupting volcanoes</li>
                <li>Unrest volcanoes</li>
                <li>Historical records</li>
                <li>Geospatial data</li>
                <li>Recent earthquakes dataset</li>
            </ul>
        </li>
        <li>PostGIS for geographic queries</li>
    </ul>
    </div>
    """, unsafe_allow_html=True)

# Visualization
st.markdown("""
<div class="component">
<span class="icon">📊</span>
<strong>Visualization (Streamlit Dashboard)</strong>
<ul>
    <li>Interactive world map of active volcanoes</li>
    <li>Information cards with key metrics:
        <ul>
            <li>Currently erupting and unrest volcanoes</li>
            <li>Population exposure estimates</li>
            <li>...</li>
        </ul>
    </li>
    <li>Time-series analysis of volcanic activity</li>
</ul>
</div>
""", unsafe_allow_html=True)

# Technical Stack
st.markdown('<h2 class="section-header">💻 Technical Stack</h2>', unsafe_allow_html=True)

st.markdown("""
<div class="tech-stack">
<ul style="columns: 2; column-gap: 30px;">
    <li><strong>Python / Web Scraping:</strong> Pandas, Requests, BeautifulSoup, SQLAlchemy, GeoPandas</li>
    <li><strong>Orchestration:</strong> Apache Airflow (DAGs, operators, sensors)</li>
    <li><strong>Database:</strong> PostgreSQL, PostGIS (spatial extensions)</li>
    <li><strong>Visualization:</strong> Streamlit, PyDesck</li>
    <li><strong>Monitoring:</strong> Airflow UI, logging</li>
    <li><strong>Version Control:</strong> Git</li>
</ul>
</div>
""", unsafe_allow_html=True)

# Future Enhancements
st.markdown('<h2 class="section-header">🚀 Future Enhancements</h2>', unsafe_allow_html=True)
st.markdown("""
<div class="component">
<ul style="columns: 2; column-gap: 30px;">
    <li>...</li>
</ul>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="author-card">
    <div class="author-name">Olivier Gillet</div>
    <div class="author-title">Research Engineer</div>

    I'm a Research Engineer specializing in data pipeline development and geospatial data analysis.
    With expertise in Python/R, data orchestration, and geospatial analysis, i design systems to transform
    complex scientific data into actionable insights.

    gillet.olivier@outlook.fr
  
</div>
""", unsafe_allow_html=True)


st.markdown("""
<div style="text-align: center; margin-top: 30px; color: #7f8c8d; font-size: 0.9em;">
    © 2023 Olivier Gillet | Volcanic Data Pipeline Project
</div>
""", unsafe_allow_html=True)
