import json
from haversine import haversine
EOF = "eof"

class WeatherForEj1:
    def __init__(self, body):
        data = json.loads(body)
        self._type = data["type"]
        self._eof = True if self._type == EOF else False
        self._city = None
        if self._eof == False:
            self._city = data["city"]
            self._date = data["date"]

class TripsForEj1:
    def __init__(self, body):
        data = json.loads(body)
        self._type = data["type"]
        self._eof = True if self._type == EOF else False
        self._city = None
        self._start_date = None
        self._duration_sec = None
        if self._eof == False:
            self._city = data["city"]
            self._start_date = data["start_date"]
            self._duration_sec = data["duration_sec"]

class StationsDataForEj2:
    def __init__(self, body):
        data = json.loads(body)
        self._type = data["type"]
        self._eof = True if self._type == EOF else False
        self._city = None
        self._code = None
        self._yearid = None
        self._name = None
        if self._eof == False:
            self._city = data["city"]
            self._code = data["code"]
            self._yearid = data["yearid"]
            self._name = data["name"]

class TripsForEj2:
    def __init__(self, body):
        data = json.loads(body)
        self._type = data["type"]
        self._eof = True if self._type == EOF else False
        self._city = None
        self._start_station_code = None
        self._yearid = None
        if self._eof == False:
            self._city = data["city"]
            self._start_station_code = data["start_station_code"]
            self._yearid = data["yearid"]

class StationsDataForEj3:
    def __init__(self, body):
        data = json.loads(body)
        self._type = data["type"]
        self._eof = True if self._type == EOF else False
        self._code = None
        self._yearid = None
        self._name = None
        self._latitude = None
        self._longitude = None
        if self._eof == False:
            self._code = data["code"]
            self._yearid = data["yearid"]
            self._name = data["name"]
            self._latitude = data["latitude"]
            self._longitude = data["longitude"]

class TripsForEj3:
    def __init__(self, body):
        data = json.loads(body)
        self._type = data["type"]
        self._eof = True if self._type == EOF else False
        self._start_station_code = None
        self._end_station_code = None
        self._yearid = None
        if self._eof == False:
            self._start_station_code = data["start_station_code"]
            self._end_station_code = data["end_station_code"]
            self._yearid = data["yearid"]

class DayWithMoreThan30mmPrectot:
    def __init__(self):
        self._n_trips = 0
        self._total_duration = 0.0

    def add_trip(self, duration):
        self._n_trips += 1
        self._total_duration += duration

    def add_trips(self, n, duration):
        self._n_trips += n
        self._total_duration += duration

    def get_average_duration(self):
        try:
            return self._total_duration / self._n_trips
        except ZeroDivisionError:
            return 0.0

class Station16Or17:
    def __init__(self):
        self._trips_on_2016 = 0
        self._trips_on_2017 = 0
    
    def add_trip(self, year):
        if year == "2016":
            self._trips_on_2016 += 1
        elif year == "2017":
            self._trips_on_2017 += 1

    def add_trips(self, year, n):
        if year == "2016":
            self._trips_on_2016 += n
        elif year == "2017":
            self._trips_on_2017 += n

    def duplicate_trips(self):
        return (self._trips_on_2016 * 2 <= self._trips_on_2017) and self._trips_on_2017 != 0

class MontrealStation:
    def __init__(self, latitude, longitude):
        self._latitude = float(latitude)
        self._longitude = float(longitude)

        self._trips = 0
        self._total_km_to_come = 0.0

    def add_trips(self, n, km):
        self._trips += n
        self._total_km_to_come += km
    
    def add_trip(self, origin):
        end = (self._latitude, self._longitude)
        distance_in_km = haversine(origin, end)

        self._trips += 1
        self._total_km_to_come += distance_in_km

    def get_average_km(self):
        try:
            return self._total_km_to_come / self._trips
        except ZeroDivisionError:
            return 0.0
