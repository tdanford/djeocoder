import ebpub.gecoder.base.Geocoder

class ChainingGeocoder(ebpub.geocoder.base.Geocoder):
	def __init(self, gc):
		self.geocoder=gc
	def _do_geocode(self, location_string):
		return self.geocoder.geocode(location_string)
