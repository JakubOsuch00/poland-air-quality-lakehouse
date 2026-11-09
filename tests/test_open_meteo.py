from datetime import date

from aq_lakehouse.ingestion.open_meteo import stations_for_period

STATIONS = """Nr,Kod stacji,Kod międzynarodowy,Nazwa stacji,Stary Kod,Data uruchomienia,Data zamknięcia,Typ stacji,Typ obszaru,Rodzaj stacji,Województwo,Miejscowość,Adres,WGS84 φ N,WGS84 λ E
1,DsA,,A,,2000-01-01,,tło,miejski,x,DOLNOŚLĄSKIE,A,,51.1,17.0
2,DsB,,B,,2000-01-01,,tło,miejski,x,DOLNOŚLĄSKIE,B,,51.2,17.1
3,DsC,,C,,2000-01-01,,tło,miejski,x,DOLNOŚLĄSKIE,C,,,
"""

SENSORS = """Nr,Kod stanowiska,Kod stacji,Nazwa stacji,Wskaźnik - kod,Wskaźnik,Czas uśredniania,Typ pomiaru,Data uruchomienia,Data zamknięcia,Województwo,Nazwa strefy
1,DsA-PM10-1g,DsA,A,PM10,pył,1-godzinny,automatyczny,2010-01-01,,X,Y
2,DsB-PM10-1g,DsB,B,PM10,pył,1-godzinny,automatyczny,2010-01-01,2015-12-31,X,Y
3,DsB-NO2-1g,DsB,B,NO2,no2,1-godzinny,automatyczny,2010-01-01,,X,Y
4,DsC-PM10-1g,DsC,C,PM10,pył,1-godzinny,automatyczny,2010-01-01,,X,Y
"""


def test_stations_for_period():
    points = stations_for_period(
        STATIONS, SENSORS, {"PM10", "PM2.5"}, date(2023, 1, 1), date(2023, 12, 31)
    )
    # B only measured NO2 in 2023, C has no coordinates
    assert [p.station_code for p in points] == ["DsA"]
    assert points[0].latitude == 51.1


def test_stations_for_period_past():
    points = stations_for_period(STATIONS, SENSORS, {"PM10"}, date(2014, 1, 1), date(2014, 12, 31))
    assert [p.station_code for p in points] == ["DsA", "DsB"]
