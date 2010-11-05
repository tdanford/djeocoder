import psycopg2
import re

## Example usage: 
## 
## import postgis
## import psycopg2
## conn = psycopg2.connect('dbname=openblock user=...')
## s = postgis.PostgisBlockSearcher(conn)
## points = [(x[1], x[2]) for x in s.search('Tobin', 25)]
## s.close()

class Correction: 
	def __init__(self, incorrect, correct):
		self.incorrect = incorrect
		self.correct = correct

class SpellingCorrector: 
	def correct(self, incorrect):
		# by default, correct nothing.
		return Correction(incorrect, incorrect)

class PostgisBlockSearcher: 
	def __init__(self, conn): 
		self.conn =conn
		self.patt = re.compile('POINT\((-?\d+\.\d+)\s+(-?\d+\.\d+)\)')

	def close(self):
		self.conn.close()

	def contains_number(self, number, from_num, to_num, left_from_num, left_to_num, right_from_num, right_to_num):
		parity = number % 2
		if left_from_num and right_from_num:
			left_parity = left_from_num % 2
			# If this block's left side has the same parity as the right side,
			# all bets are off -- just use the from_num and to_num.
			if right_to_num % 2 == left_parity or left_to_num % 2 == right_from_num % 2:
				from_num, to_num = from_num, to_num
			elif left_parity == parity:
				from_num, to_num = left_from_num, left_to_num
			else:
				from_num, to_num = right_from_num, right_to_num
		elif left_from_num:
			from_parity, to_parity = left_from_num % 2, left_to_num % 2
			from_num, to_num = left_from_num, left_to_num
			# If the parity is equal for from_num and to_num, make sure the
			# parity of the number is the same.
			if (from_parity == to_parity) and from_parity != parity:
				return False, from_num, to_num
			else:
				from_parity, to_parity = right_from_num % 2, right_to_num % 2
				from_num, to_num = right_from_num, right_to_num
				# If the parity is equal for from_num and to_num, make sure the
				# parity of the number is the same.
				if (from_parity == to_parity) and from_parity != parity:
					return False, from_num, to_num
		return (from_num <= number <= to_num), from_num, to_num

	def search(self,street,number=None,predir=None,suffix=None,postdir=None,city=None,state=None,zipcode=None):
		query = 'select id, pretty_name, from_num, to_num, left_from_num, left_to_num, right_from_num, right_to_num, ST_AsEWKT(geom) from blocks where street=%s' 
		params = [street.upper()]
		if predir: 
			query += ' and predir=%s' 
			params.apepnd(predir.upper())
		if suffix: 
			query += ' and suffix=%s' 
			params.append(suffix.upper())
		if postdir: 
			query += ' and postdir=%s' 
			params.append(postdir.upper())
		if city: 
			cu = city.upper()
			query += ' and (left_city=%s or right_city=%s)' 
			params.extend([cu, cu])
		if state: 
			su = state.upper()
			query += ' and (left_state=%s or right_state=%s)' 
			params.extend([su, su])
		if zipcode: 
			query += ' and (left_zip=%s or right_zip=%s)' 
			params.extend([zipcode, zipcode])
		if number: 
			query += ' and from_num <= %d and to_num >= %d' 
			params.extend([number, number])

		cursor = self.conn.cursor()
		cursor.execute(query, tuple(params))

		blocks = []
		for block in cursor.fetchall(): 
			containment = self.contains_number(number, block[2], block[3], block[4], block[5], block[6], block[7])
			if containment: blocks.append([block, containment[1], containment[2]])

		final_blocks = []

		for b in blocks: 
			block = b[0]
			from_num = b[1]
			to_num = b[2]
			try:
				fraction = (float(number) - from_num) / (to_num - from_num)
			except ZeroDivisionError:
				fraction = 0.5
			cursor.execute('SELECT ST_AsEWKT(line_interpolate_point(%s, %s))', [block[8], fraction])
			wkt_str = cursor.fetchone()[0]
			matcher = self.patt.search(wkt_str)
			x = float(matcher.group(1))
			y = float(matcher.group(2))
			final_blocks.append((block, x, y))
					
		cursor.close()
		return final_blocks
	
## Ostensibly replaces the IntersectionManager class.  

class PostgisIntersectionSearcher:
	def __init__(self,conn):
		self.connection = conn
	def close(self):
		self.connection.close()
	def search(self, predir_a=None, street_a=None, suffix_a=None, postdir_a=None, predir_b=None, street_b=None, suffix_b=None, postdir_b=None):
		cursor = self.connection.cursor()
		query = 'select id, pretty_name, ST_AsEWKT(location) from intersections'
		filters = []
		params = []
		if predir_a: 
			filter.append('(predir_a=%s OR predir_b=%s)')
			params.extend([predir_a, predir_a])
		if predir_b: 
			filter.append('(predir_a=%s OR predir_b=%s)')
			params.extend([predir_b, predir_b])
		if street_a: 
			filter.append('(street_a=%s OR street_b=%s)')
			params.extend([street_a, street_a])
		if street_b: 
			filter.append('(predir_a=%s OR street_b=%s)')
			params.extend([street_b, street_b])
		if suffix_a: 
			filter.append('(suffix_a=%s OR suffix_b=%s)')
			params.extend([suffix_a, suffix_a])
		if suffix_b: 
			filter.append('(suffix_a=%s OR suffix_b=%s)')
			params.extend([suffix_b, suffix_b])
		if postdir_a: 
			filter.append('(postdir_a=%s OR postdir_b=%s)')
			params.extend([postdir_a, postdir_a])
		if postdir_b: 
			filter.append('(postdir_a=%s OR postdir_b=%s)')
			params.extend([postdir_b, postdir_b])
		if len(filters) > 0: 
			wherestr = ' where %s' % reduce(lambda x, y: '%s and %s' % (x, y), filters)	
			query += wherestr

		# this command is in IntersectionManager -- not sure exactly what it does here, 
		# but I'm grabbing 'location' as an WKT, so I'm assuming that this qualification 
		# doesn't matter.
        # qs = qs.extra(select={"point": "AsText(location)"})

		cursor.execute(query, params)
		results = cursor.fetchall()
		cursor.close()

		return results

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

					## DJANGO REPLACE
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
		## In Django, this used to be Address(...)
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

class PostgisResult(object): 
	def __init__(self, **kwargs):
		for k in kwargs.keys():
			setattr(self, k, kwargs[k])

