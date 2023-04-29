
import json

class Weather:
    def __init__(self, weather):
        data = json.loads(weather)
        self.city = data["city"]
        self.date = data["date"]
        self.prectot = data["prectot"]
        self.qv2m = data["qv2m"]
        self.rh2m = data["rh2m"]
        self.ps = data["ps"]
        self.t2m_range = data["t2m_range"]
        self.ts = data["ts"]
        self.t2mdew = data["t2mdew"]
        self.t2mwet = data["t2mwet"]
        self.t2m_max = data["t2m_max"]
        self.t2m_min = data["t2m_min"]
        self.t2m = data["t2m"]
        self.ws50m_range = data["ws50m_range"]
        self.ws10m_range = data["ws10m_range"]
        self.ws50m_min = data["ws50m_min"]
        self.ws10m_min = data["ws10m_min"]
        self.ws50m_max = data["ws50m_max"]
        self.ws10m_max = data["ws10m_max"]
        self.ws50m = data["ws50m"]
        self.ws10m = data["ws10m"]
    
    def get_weather_for_ej1filter(self):
        return json.dumps({
            "city": self.city,
            "date": self.date,
            "prectot": self.prectot,
        })

class Station:
    def __init__(self, station):
        data = json.loads(station)
        self.city = data["city"]
        self.code = data["code"]
        self.name = data["name"]
        self.latitude = data["latitude"]
        self.longitude = data["longitude"]
        self.yearid = data["yearid"]

    def get_station_for_ej2solver(self):
        return json.dumps({
            "type": "stations",
            "city": self.city,
            "code": self.code,
            "name": self.name,
            "yearid": self.yearid,
        })

    def get_station_for_ej3filter(self):
        return json.dumps({
            "city": self.city,
            "code": self.code,
            "name": self.name,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "yearid": self.yearid,
        })

class Trip:
    def __init__(self, trip):
        data = json.loads(trip)
        self.city = data["city"]
        self.start_date = data["start_date"]
        self.start_station_code = data["start_station_code"]
        self.end_date = data["end_date"]
        self.end_station_code = data["end_station_code"]
        self.duration_sec = data["duration_sec"]
        self.is_member = data["is_member"]
        self.yearid = data["yearid"]

    def get_trip_for_ej1solver(self):
        return json.dumps({
            "type": "trips",
            "city": self.city,
            "start_date": self.start_date,
            "duration_sec": self.duration_sec,
        })

    def get_trip_for_ej2filter(self):
        return json.dumps({
            "city": self.city,
            "start_station_code": self.start_station_code,
            "yearid": self.yearid,
        })

    def get_trip_for_ej3filter(self):
        return json.dumps({
            "type": "trips",
            "city": self.city,
            "start_station_code": self.start_station_code,
            "end_station_code": self.end_station_code,
            "yearid": self.yearid,
        })

class EOF:
    def __init__(self, topic):
        self.topic = topic
    
    def get_json(self):
        return json.dumps({
            "type": "eof",
            "eof": self.topic,
        })
