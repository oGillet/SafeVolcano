import folium
import pydeck as pdk
import pandas as pd
import streamlit as st
import pandas as pd
from numpy.random import default_rng as rng
from streamlit_folium import st_folium

# Data sources

st.image("ETL/app/data/images/image_background.jpg", caption="La Soufrière de Guadeloupe (France)")

st.title("Interactive Map of Active Volcanoes")


st.set_page_config(
    page_title="Active Volcanoes & Recent Earthquake",
    page_icon="⚠️"
)

st.markdown(
    """
    **Data Pipeline (ETL with Python, Airflow, SQL, and Streamlit)**
    
    It's my first ETL (Extract, Transform, Load) pipeline, designed to collect, process, and visualize data on erupting and unrest volcanoes from the Smithsonian Institution’s Global Volcanism Program (GVP).
    
    Key Components:
    
    * Data Extraction
        - Automated extraction of structured data (e.g., eruption dates, volcano locations) from the Smithsonian’s public datasets (https://volcano.si.edu/ - web scraping).
    
    * Transformation & Enrichment
        - *Data cleaning*: Handling missing values, standardizing formats (e.g., dates, coordinates), and removing duplicate rows.
        - *Joins/Merges*: Combining eruption data with the datasets produced by the Smithsonian Institution (e.g., geological metadata, historical records) to add context (e.g., volcano type, exposed inhabitants).
    
    * Orchestration with Apache Airflow
        - Scheduling and monitoring the pipeline (daily updates).
        - Running transformations only after successful extraction.
    
    * Storage (SQL Database)
        - Loading processed data into a relational database (PostgreSQL, PostGIS).
        - Example tables:
            - eurpting volcanoes
            - unrest volcanoes
            - ...
    
    * Visualization & Dashboard (Streamlit)
        - Interactive dashboard to explore:
            - Map of active volcanoes.
            - Cards with informations (e.g. exposed people)
    
    * Technical Stack:
        - **Python**: Pandas (data manipulation), Requests/BeautifulSoup (scraping), SQLAlchemy (DB interaction), etc ...
        - **Apache Airflow**: DAGs for workflow automation.
        - **SQL**: Database design, complex joins, and aggregations.
        - **Streamlit**: Frontend for visualization.

    """
)
