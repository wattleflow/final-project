# Pozadina

Raspolozivi podaci na US [data.gov](https://catalog.data.gov/dataset) sadrzavaju razne tabele. 
Povrsni pregled kataloga ukazuje na mogucnost kombinacije podataka objavljenih od strane grada `New York-a`. 

Primjerice, [demografska statistika](https://catalog.data.gov/dataset/demographic-statistics-by-zip-code) i
[GiS Naselja](https://catalog.data.gov/dataset/neighborhood-names-gis) sa [kriminalom iz mrznje](https://catalog.data.gov/dataset/nypd-hate-crimes) te gdje su se dogadjala
[hapsenja](https://catalog.data.gov/dataset/nypd-arrest-data-year-to-date).

Vjerovatnost je da se ovi podaci mogu kombinirati, za sto je neophodno napraviti data model. 
Dalje, ove podatke je moguce objediniti u graph bazu podatka, te je takve dalje moguce pretociti u vizualni dashboard. 
Za ovo je preporucljivo koristiti Cloud platformu sa podrskom za mape, poput Google ili Azure (Bing).

# Planiranje
1. Opensource potraga
2. Analize podataka
3. Izrada dags modela
4. Izrada star modela 

## 1. Opensource potraga
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
