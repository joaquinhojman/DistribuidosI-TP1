import json

EOF = "eof"
EJSOLVER = "EjSolver"
RESULTS = "results"
TRIPS = "trips"

class Data:
    def __init__(self, data):
        info = data.split(";")
        self.topic = info[0]
        self.eof = True if info[1] == "1" else False
        self.data = None
        if (self.eof == False):
            self.data = info[2]

class EOF:
    def __init__(self, data):
        data = json.loads(data)
        self.EjSolver = data[EJSOLVER]
        self.eof = data[EOF]
        self.results = data[RESULTS] if self.eof == TRIPS else None
