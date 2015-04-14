
class Counter:

	properties = {}

	keys = []

	def __init__(self):
		self.properties = {}
		self.keys = []
	
	def register_property(self,key):
		if key in self.keys:
			return
		else:
			self.keys.append(key)

	def remove_property(self,key):
		if key in self.keys:
			keys.remove(key)
		if key in self.properties:
			properties.remove(key)

	def add_property(self,key,value):
		if key in self.keys:
			self.properties[key] = value
			return 1
		else:
			return 0

	def get_value(self,key):
		if not key in self.keys:
			return
		return self.properties[key]


