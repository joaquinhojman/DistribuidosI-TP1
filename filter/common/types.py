import json

WEATHER = "weather"
STATIONS = "stations"
TRIPS = "trips"
_EOF = "eof"

class We1:
    def __init__(self, we1):
        data = json.loads(we1)
        self.city = data["city"]
        self.date = data["date"]
        self.prectot = float(data["prectot"])

    def is_valid(self):
        return self.prectot >= 30.00

    def get_json(self):
        return json.dumps({
            "type": WEATHER,
            "city": self.city,
            "date": self.date,
        })

class Se2:
    def __init__(self, se2):
        data = json.loads(se2)
        self.city = data["city"]
        self.code = data["code"]
        self.name = data["name"]
        self.yearid = data["yearid"]
    
    def is_valid(self):
        return self.yearid == "2016" or self.yearid == "2017"

    def get_json(self):
        return json.dumps({
            "type": STATIONS,
            "city": self.city,
            "code": self.code,
            "name": self.name,
            "yearid": self.yearid,
        })

class Te2:
    def __init__(self, te2):
        data = json.loads(te2)
        self.city = data["city"]
        self.start_station_code = data["start_station_code"]
        self.yearid = data["yearid"]

    def is_valid(self):
        return self.yearid == "2016" or self.yearid == "2017"

    def get_json(self):
        return json.dumps({
            "type": TRIPS,
            "city": self.city,
            "start_station_code": self.start_station_code,
            "yearid": self.yearid,
        })

class Se3:
    def __init__(self, se3):
        data = json.loads(se3)
        self.city = data["city"]
        self.code = data["code"]
        self.name = data["name"]
        self.latitude = data["latitude"]
        self.longitude = data["longitude"]
        self.yearid = data["yearid"]
    
    def is_valid(self):
        return self.city == "montreal"

    def get_json(self):
        return json.dumps({
            "type": STATIONS,
            "code": self.code,
            "name": self.name,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "yearid": self.yearid,
        })

class Te3:
    def __init__(self, te3):
        data = json.loads(te3)
        self.city = data["city"]
        self.start_station_code = data["start_station_code"]
        self.end_station_code = data["end_station_code"]
        self.yearid = data["yearid"]
    
    def is_valid(self):
        return self.city == "montreal"

    def get_json(self):
        return json.dumps({
            "type": TRIPS,
            "start_station_code": self.start_station_code,
            "end_station_code": self.end_station_code,
            "yearid": self.yearid,
        })

class EOF:
    def __init__(self, topic):
        self.topic = topic
    
    def get_json(self):
        return json.dumps({
            "type": _EOF,
            "eof": self.topic,
        })
