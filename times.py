class Time:
	
    time = ""
    def __init__(self,time_str):
        self.time = time_str

    def __strtoint__(self):
		return long(self.time)

    def __sub__(self,other):
		if other.__strtoint__() > self.__strtoint__():
			return other.__strtoint__()- self.__strtoint__()
		else:
			return self.__strtoint__() - other.__strtoint__()

    def get_second(self):
		return self.__strtoint__() / 1000;

    def get_minute(self):
		return self.get_second() / 60;
