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
		query = 'select id, pretty_name, from_num, to_num, left_from_num, left_to_num, right_from_num, right_to_num, ST_AsEWKT(geom) from blocks where street=\'%s\'' % street.upper()	
		if predir: query += ' and predir=\'%s\'' % predir.upper()
		if suffix: query += ' and suffix=\'%s\'' % predir.upper()
		if postdir: query += ' and postdir=\'%s\'' % predir.upper()
		if city: 
			cu = city.upper()
			query += ' and (left_city=\'%s\' or right_city=\'%s\')' % cu
		if state: 
			su = state.upper()
		query += ' and (left_state=\'%s\' or right_state=\'%s\')' % su
		if zipcode: 
			query += ' and (left_zip=\'%s\' or right_zip=\'%s\')' % zipcode
		if number: 
			query += ' and from_num <= %d and to_num >= %d' % (number, number)

		cursor = self.conn.cursor()
		cursor.execute(query)

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


