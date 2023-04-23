
class Data:
    def __init__(self, data):
        info = data.split(";")
        self.topic = info[0]
        self.eof = True if info[1] == "1" else False
        self.data = None
        if (self.eof == False):
            self.data = info[2]
