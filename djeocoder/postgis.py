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
