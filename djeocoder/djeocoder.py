from exceptions import Exception
from parser.parsing import normalize, parse, ParsingError

import re

# from streets import Block, StreetMisspelling, Intersection
# from geocoder_models import GeocoderCache

class GeocoderException(Exception):
    pass

block_re = re.compile(r'^(\d+)[-\s]+(?:blk|block)\s+(?:of\s+)?(.*)$', re.IGNORECASE)
intersection_re = re.compile(r'(?<=.) (?:and|\&|at|near|@|around|towards?|off|/|(?:just )?(?:north|south|east|west) of|(?:just )?past) (?=.)', re.IGNORECASE)

class LocalGeocoder: 
    def geocode(self, location):
        cxn = ## ? 
        
        if intersection_re.search(location):
            #raise GeocoderException('Intersection geocoding not implemented')
            geocoder = PostgisIntersectionGeocoder(cxn)

        elif block_re.search(location):
            #raise GeocoderException('Block geocoding not implemented')
            geocoder = PostgisBlockGeocoder(cxn)

        else:
            geocoder = PostgisAddressGeocoder(cxn)

        return geocoder.geocode(location)

## A replacement for AddressGeocoder
class PostgisAddressGeocoder:
	def __init__(self, cxn):
		self.connection = cxn
		self.spelling = SpellingCorrector()

    def geocode(self, location_string):
        # Parse the address.
        try:
            locations = parse(location_string)
        except ParsingError, e:
            raise

        all_results = []
        for loc in locations:
            loc_results = self._db_lookup(loc)

            # If none were found, maybe the street was misspelled. Check that.
            if not loc_results and loc['street']:
                try:
                    # Originally, StreetMisspelling.objects would hit the database for a list of corrected
                    # street names.  Now, we route this through an interface instead.  
                    # 
                    # misspelling = StreetMisspelling.objects.get(incorrect=loc['street'])
                    #   -> should return, now, a Correction object.
                    misspelling = self.spelling.correct(incorrect=loc['street'])
                    loc['street'] = misspelling.correct
                    
                except StreetMisspelling.DoesNotExist:
                    pass
                else:
                    loc_results = self._db_lookup(loc)
                
                # Next, try removing the street suffix, in case an incorrect
                # one was given.
                if not loc_results and loc['suffix']:
                    loc_results = self._db_lookup(dict(loc, suffix=None))
                
                # Next, try looking for the street, in case the street
                # exists but the address doesn't.
                if not loc_results and loc['number']:
                    kwargs = {'street': loc['street']}
                    sided_filters = []
                    if loc['city']:
                        city_filter = Q(left_city=loc['city']) | Q(right_city=loc['city'])
                        sided_filters.append(city_filter)
                        # DJANGOism: replace
                        # b_list = Block.objects.filter(*sided_filters, **kwargs).order_by('predir', 'from_num', 'to_num')
                        PostgisBlockSearcher searcher = PostgisBlockSearch(self.connection)
                        b_list = searcher.search(*sided_filters, **kwargs)
                        searcher.close()

                    if b_list:
                        raise InvalidBlockButValidStreet(loc['number'], b_list[0].street_pretty_name, b_list)

            all_results.extend(loc_results)

        if not all_results:
            raise DoesNotExist("Geocoder db couldn't find this location: %r" % location_string)
        elif len(all_results) == 1:
            return all_results[0]
        else:
            raise AmbiguousResult(all_results)

    def _db_lookup(self, location):
        """
        Given a location dict as returned by parse(), looks up the address in
        the DB. Always returns a list of Address dictionaries (or an empty list
        if no results are found).
        """
        if not location['number']:
            return []

        # Query the blocks table in the database.
        try:
            searcher = PostgisBlockSearch(self.connection)
            blocks = searcher.search(
                street=location['street'],
                number=location['number'],
                predir=location['pre_dir'],
                suffix=location['suffix'],
                postdir=location['post_dir'],
                city=location['city'],
                state=location['state'],
                zipcode=location['zip'],
            )
            searcher.close()

        except Exception, e:
            # TODO: replace with Block-specific exception
            raise Exception("Road segment db query failed: %r" % e)
        return [self._build_result(location, block, geocoded_pt) for block, geocoded_pt in blocks]

    def _build_result(self, location, block, geocoded_pt):
        # In Django, this used to be Address(...)
        return PostgisResult(**{
            'address': unicode(" ".join([str(s) for s in [location['number'], block.predir, block.street_pretty_name, block.postdir] if s])),
            'city': block.city.title(),
            'state': block.state,
            'zip': block.zip,
            'block': block,
            'intersection_id': None,
            'point': geocoded_pt,
            'url': block.url(),
            'wkt': str(block.location),
        })

## Copied from ebpub.base.BlockGeocoder
class PostgisBlockGeocoder(PostgisAddressGeocoder):
    def _do_geocode(self, location_string):
        m = block_re.search(locationstring)
        if not m:
            # TODO: replace with Block-specific exception
            raise ParsingError("BlockGeocoder somehow got an address it can't parse: %r" % location_string)
        new_location_string = ' '.join(m.groups())
        return PostgisAddressGeocoder.geocode(self, new_location_string)

## A replacement for ebpub.base.IntersectionGeocoder
class PostgisIntersectionGeocoder:
    def __init__(self, cxn):
        self.connection = cxn
        self.spelling = SpellingCorrector()

    def geocode(self, location_string):
        sides = intersection_re.split(location_string)
        if len(sides) != 2:
            raise ParsingError("Couldn't parse intersection: %r" % location_string)

        # Parse each side of the intersection to a list of possibilities.
        # Let the ParseError exception propagate, if it's raised.
        left_side = parse(sides[0])
        right_side = parse(sides[1])

        all_results = []
        seen_intersections = set()
        for street_a in left_side:
            street_a['street'] = self.spelling.correct(street_a['street'])
            for street_b in right_side:
                street_b['street'] = self.spelling.correct(street_b['street'])
                for result in self._db_lookup(street_a, street_b):
                    if result["intersection_id"] not in seen_intersections:
                        seen_intersections.add(result["intersection_id"])
                        all_results.append(result)

        if not all_results:
            raise DoesNotExist("Geocoder db couldn't find this intersection: %r" % location_string)
        elif len(all_results) == 1:
            return all_results.pop()
        else:
            raise AmbiguousResult(list(all_results), "Intersections DB returned %s results" % len(all_results))

    def _db_lookup(self, street_a, street_b):
        try:
            searcher = PostgisIntersectionSearcher(self.connection)
            intersections = searcher.search(
                predir_a=street_a['pre_dir'],
                street_a=street_a['street'],
                suffix_a=street_a['suffix'],
                postdir_a=street_a['post_dir'],
                predir_b=street_b['pre_dir'],
                street_b=street_b['street'],
                suffix_b=street_b['suffix'],
                postdir_b=street_b['post_dir'],
            )
            searcher.close()
        # except Exception, e:
        except DoesNotExist, e:
            raise DoesNotExist("Intersection db query failed: %r" % e)
        return [self._build_result(i) for i in intersections]

    def _build_result(self, intersection):
        return PostgisResult(**{
            'address': intersection.pretty_name,
            'city': intersection.city,
            'state': intersection.state,
            'zip': intersection.zip,
            'intersection_id': intersection.id,
            'intersection': intersection,
            'block': None,
            'point': intersection.location,
            'url': intersection.url(),
            'wkt': str(intersection.location),
        })

class PostgisResult(object): 
	def __init__(self, **kwargs):
		for k in kwargs.keys():
			setattr(self, k, kwargs[k])

class GeocodingException(Exception):
    pass


