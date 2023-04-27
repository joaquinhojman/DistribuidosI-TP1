import json

class We1:
    def __init__(self, we1):
        data = json.loads(we1)
        self.date = data["date"]
        self.prectot = float(data["prectot"])

    def is_valid(self):
        return self.prectot >= 30.00

    def get_json(self):
        return json.dumps({
            "type" : "weather",
            "date": self.date,
            #"prectot": self.prectot,
        })

class Te2:
    def __init__(self, te2):
        data = json.loads(te2)
        self.start_station_code = data["start_station_code"]
        self.start_date = data["start_date"]

    def is_valid(self):
        return self.start_date[:4] == "2016" or self.start_date[:4] == "2017"

    def get_json(self):
        return json.dumps({
            "type" : "trip",
            "start_station_code": self.start_station_code,
            "start_date_year": self.start_date[:4],
        })

class Se3:
    def __init__(self, se3):
        data = json.loads(se3)
        self.city = data["city"]
        self.code = data["code"]
        self.name = data["name"]
        self.latitude = data["latitude"]
        self.longitude = data["longitude"]
    
    def is_valid(self):
        return self.city == "Montreal"

    def get_json(self):
        return json.dumps({
            "type" : "station",
            "city": self.city,
            "code": self.code,
        })
