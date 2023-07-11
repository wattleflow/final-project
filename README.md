# Background

The available data on US [data.gov](https://catalog.data.gov/dataset) contains various tables. A review of the catalog indicates the possibility of combining data published by the New York Police Department (NYPD) using their CompStat software, known for the "Broken Windows" theory.

For example, the recorded [GiS locations](https://catalog.data.gov/dataset/neighborhood-names-gis) of shooting incidents, 911 calls, and the [location of arrested](https://catalog.data.gov/dataset/nypd-arrest-data-year-to-date) criminals in New York neighborhoods.

It is likely that these data can be combined, but it requires a detailed analysis to create a valid data model. Furthermore, these data can be visually represented through maps, dashboards, or scatter plot graphs. The data can also be integrated into a graph database.

For analysis, tools like Docker with Apache Airflow and a PostgresSQL database can be used. As for the final solution, a cloud platform that supports working with maps, such as Google or Azure (Bing), can be utilized.

# Planning
1. Opensource research
2. Data analysis
3. Developing dags model
4. Developing star model 

## 1. Opensource search
1. [Neighbourhoods](https://catalog.data.gov/dataset/neighborhood-names-gis/resource/187b8e9e-2a76-42b4-8253-f3a0be0169e7)
2. [Mental-health](https://catalog.data.gov/dataset/mental-health-care-in-the-last-4-weeks/resource/803b3b82-0f92-43d8-a146-46d2307cc2e9)
3. [Demographics](https://catalog.data.gov/dataset/demographic-statistics-by-zip-code/resource/d32826e6-db5c-4a24-9930-1693947e4e1f)
4. [Air quality](https://catalog.data.gov/dataset/air-quality/resource/f3ed1638-92da-4f88-bb6b-7d3940514574)
5. [Shootings](https://catalog.data.gov/dataset/nypd-shooting-incident-data-historic/resource/c564b578-fd8a-4005-8365-34150d306cc4)
6. [Arrests historic](https://catalog.data.gov/dataset/nypd-arrests-data-historic/resource/08c24036-1e4a-4dc1-82ad-21a2ef833aa9)
7. [Pregnancy](https://catalog.data.gov/dataset/pregnancy-associated-mortality/resource/0937907b-9651-4cca-b322-c3b96733ebcd)
8. [Hate crimes](https://catalog.data.gov/dataset/nypd-hate-crimes/resource/6a431837-5576-420d-8857-e1beee49de2d)
9. [Subways](https://catalog.data.gov/dataset/subway-stations/resource/09799f38-17c6-4f19-bf27-d88c38b86193)
10. [NYPD Calls](https://catalog.data.gov/dataset/nypd-calls-for-service/resource/8c6b95bb-0589-4df2-9745-4a4441c7c06e)
11. [Arrests](https://catalog.data.gov/dataset/nypd-arrest-data-year-to-date/resource/c48f1a1a-5efb-4266-9572-769ed1c9b472)
12. [Post Office](https://catalog.data.gov/dataset/post-office/resource/4f6b4fbd-3e07-444a-ad9a-851ea18828f7)
13. [Covid19](https://data.cityofnewyork.us/api/views/rc75-m7u3/rows.csv?accessType=DOWNLOAD)

## 2. Analize podataka
- NYPD Calls (8) je najveca tablica sa skoro 2M zapisa
- Ova tablica centralna je za zadatak
- Tablica sadrzava nekoliko podataka za povezivanje:
  - 

# Izrada modela
- Izrada Airflow pipelines i testiranje
- Odluka o arhitekturi i platformi
- Testiranje rada platforme
- Testiranje pipelines
- Izrada dokumentacije

# Dodatni reursi
- https://github.com/san089/goodreads_etl_pipeline
- https://github.com/san089/Udacity-Data-Engineering-Projects
- https://catalog.data.gov/dataset/mental-health-care-in-the-last-4-weeks
- https://catalog.data.gov/dataset/neighborhood-names-gis
- https://catalog.data.gov/dataset/hud-low-and-moderate-income-areas
- https://developers.facebook.com/search/?q=locations&notfound=0&search_filter_option=docs
- https://developers.facebook.com/docs/graph-api/get-started
- https://github.com/toddmotto/public-apis/tree/master
- https://developers.facebook.com/docs/groups-api/guides#getting-groups-for-a-user
- https://catalog.data.gov/dataset/tiger-line-shapefile-2019-2010-nation-u-s-2010-census-5-digit-zip-code-tabulation-area-zcta5-na
- https://catalog.data.gov/dataset/500-cities-city-boundaries
- https://catalog.data.gov/dataset/hud-low-and-moderate-income-areas
- https://github.com/nareshk1290/Udacity-Data-Engineering
- https://www.kaggle.com/code/mohamedtahaouf/european-soccer-sql
