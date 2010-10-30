from exceptions import Exception
from parser.parsing import normalize, parse, ParsingError

import re

from streets import Block, StreetMisspelling, Intersection

from geocoder_models import GeocoderCache

class GeocoderException(Exception):
    pass

class AddressGeocoder:
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
                    misspelling = StreetMisspelling.objects.get(incorrect=loc['street'])
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
                    b_list = Block.objects.filter(*sided_filters, **kwargs).order_by('predir', 'from_num', 'to_num')
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

        # Query the blocks database.
        try:
            blocks = Block.objects.search(
                street=location['street'],
                number=location['number'],
                predir=location['pre_dir'],
                suffix=location['suffix'],
                postdir=location['post_dir'],
                city=location['city'],
                state=location['state'],
                zipcode=location['zip'],
            )
        except Exception, e:
            # TODO: replace with Block-specific exception
            raise Exception("Road segment db query failed: %r" % e)
        return [self._build_result(location, block, geocoded_pt) for block, geocoded_pt in blocks]

    def _build_result(self, location, block, geocoded_pt):
        return Address({
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

block_re = re.compile(r'^(\d+)[-\s]+(?:blk|block)\s+(?:of\s+)?(.*)$', re.IGNORECASE)
intersection_re = re.compile(r'(?<=.) (?:and|\&|at|near|@|around|towards?|off|/|(?:just )?(?:north|south|east|west) of|(?:just )?past) (?=.)', re.IGNORECASE)
class LocalGeocoder: 
    def geocode(self, location):
        if intersection_re.search(location):
            raise GeocoderException('Intersection geocoding not implemented')
        elif block_re.search(location):
            raise GeocoderException('Block geocoding not implemented')
        else:
            geocoder = AddressGeocoder()
        return geocoder.geocode(location)
