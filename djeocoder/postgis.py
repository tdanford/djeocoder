import psycopg2
import re

from parser.parsing import normalize, parse, ParsingError

point_pattern = re.compile('POINT\((-?\d+\.\d+)\s+(-?\d+\.\d+)\)')
def parse_point(wkt_str):
    matcher = point_pattern.search(wkt_str)
    if matcher==None: raise PointParsingException(wkt_str)
    x = float(matcher.group(1))
    y = float(matcher.group(2))
    return x, y

class Correction:
    def __init__(self, incorrect, correct):
        self.incorrect = incorrect
        self.correct = correct

class SpellingCorrector: 
    def correct(self, incorrect):
        # by default, correct nothing.
        return Correction(incorrect, incorrect)

# TODO: There's also a GeocoderException class in djeocoder.py
# -- these should probably be merged.
class GeocodingException(Exception):
    pass

class DoesNotExist(GeocodingException):
    pass

class PointParsingException(Exception):
    def __init__(self, str):
        self.str = str
    def __repr__(self):
        return 'String \'%s\' could not be parsed into points.' % self.str

# I'd like the Searcher classes to return well-defined objects,
# rather than raw tuples from the database.
class LocatableResult:
    def __init__(self, location):
        self.location = parse_point(location)
    def __repr__(self):
        return '(%.5f,%.5f)' % (self.location[0], self.location[1])

class BlockResult(LocatableResult):
    """
    Objects of this class are returned by the PostgisBlockSearcher.search() method. 
    """
    def __init__(self, block_tuple, location):
        LocatableResult.__init__(self, location)
        self.id = block_tuple[0]
        self.pretty_name = block_tuple[1]
        self.from_num = block_tuple[2]
        self.to_num = block_tuple[3]
        self.left_from_num = block_tuple[4]
        self.left_to_num = block_tuple[5]
        self.right_from_num = block_tuple[6]
        self.right_to_num = block_tuple[7]

    def __repr__(self):
        return '%s %s' % ( self.pretty_name, LocatableResult.__repr__(self) )
        
    def contains_number(self, number):
        parity = number % 2
        fn, tn = self.from_num, self.to_num
        
        if self.left_from_num and self.right_from_num:
            left_parity = self.left_from_num % 2

            # If this block's left side has the same parity as the right side,
            # all bets are off -- just use the from_num and to_num.
            
            if self.right_to_num % 2 == left_parity or self.left_to_num % 2 == self.right_from_num % 2:
                fn, tn = self.from_num, self.to_num
            elif left_parity == parity:
                fn, tn = self.left_from_num, self.left_to_num
            else:
                fn, tn = self.right_from_num, self.right_to_num
                
        elif self.left_from_num:
            from_parity, to_parity = self.left_from_num % 2, self.left_to_num % 2
            fn, tn = self.left_from_num, self.left_to_num
            
            # If the parity is equal for from_num and to_num, make sure the
            # parity of the number is the same.
            if (from_parity == to_parity) and from_parity != parity:
                return False, fn, tn
            else:
                from_parity, to_parity = self.right_from_num % 2, self.right_to_num % 2
                fn, tn = self.right_from_num, self.right_to_num
                
                # If the parity is equal for from_num and to_num, make sure the
                # parity of the number is the same.
                if (from_parity == to_parity) and from_parity != parity:
                    return False, fn, tn
        return (fn <= number <= tn), fn, tn

class IntersectionResult(LocatableResult):
    """
    Objects of this class are returned by the PostgisIntersectionSearcher.search() method.
    """
    def __init__(self, intersection_tuple):
        LocatableResult.__init__(self, intersection_tuple[2])
        self.id = intersection_tuple[0]
        self.pretty_name = intersection_tuple[1]
    def __repr__(self):
        return '%s %s' % (self.pretty_name, LocatableResult.__repr__(self))

class PostgisBlockSearcher:
    """
    Replaces the everyblock class \"BlockManager\".
    Handles interaction with the underlying database, taking a call to the search() method, converting it into a query,
    and then forming the response rows into BlockResult objects.
    """
    def __init__(self, conn): 
        self.conn =conn
        
    def close(self):
        # self.conn.close()
        pass

    def contains_number(self, number, from_num, to_num, left_from_num, left_to_num, right_from_num, right_to_num):
        """
        Copied almost verbatim from the corresponding EveryBlock class.
        
        Attempts to discover whether a particular triple of ranges
          [ (from_num, to_num), (left_from_num, left_to_num), (right_from_num, right_to_num) ]
        contains the given number.  The trick is that the number's parity may not match the parity of either the corresponding
        left or right range...
        """
        if not number: return True, from_num, to_num
        
        parity = int(number) % 2
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

    def search(self,street,number=None,pre_dir=None,suffix=None,post_dir=None,city=None,state=None,zip=None,left_city=None,right_city=None):
        query = 'select id, pretty_name, from_num, to_num, left_from_num, left_to_num, right_from_num, right_to_num, ST_AsEWKT(geom) from blocks where street=%s' 
        params = [street.upper()]
        if pre_dir: 
            query += ' and predir=%s' 
            params.apepnd(pre_dir.upper())
        if suffix: 
            query += ' and suffix=%s' 
            params.append(suffix.upper())
        if post_dir: 
            query += ' and postdir=%s' 
            params.append(post_dir.upper())
        if city: 
            cu = city.upper()
            query += ' and (left_city=%s or right_city=%s)'
            params.extend([cu, cu])
        if state: 
            su = state.upper()
            query += ' and (left_state=%s or right_state=%s)' 
            params.extend([su, su])
        if zip: 
            query += ' and (left_zip=%s or right_zip=%s)' 
            params.extend([zip, zip])
        if number: 
            query += ' and from_num <= %s and to_num >= %s' 
            params.extend([number, number])

        cursor = self.conn.cursor()
        cursor.execute(query, tuple(params))

        blocks = []
        for block in cursor.fetchall(): 
            containment = self.contains_number(number, block[2], block[3], block[4], block[5], block[6], block[7])
            if containment[0]: blocks.append([block, containment[1], containment[2]])
            
        final_blocks = []
        
        for b in blocks: 
            block = b[0]
            from_num = b[1]
            to_num = b[2]
            try:
                fraction = (float(number) - from_num) / (to_num - from_num)
            except TypeError:
                # TODO: revisit this clause.  We're getting here because the 'number' field was zero.  What do
                # we do in this case?  What does the original code do? 
                fraction = 0.5
            except ZeroDivisionError:
                fraction = 0.5

            # TODO: when we want to extract the geocoder from dependence on
            # Postgis, this is one of the main dependencies: we'll need to introduce
            # a new GIS library, so that we can do this interpolation "in code" -TWD
            cursor.execute('SELECT ST_AsEWKT(line_interpolate_point(%s, %s))', [block[8], fraction])
            wkt_str = cursor.fetchone()[0]
            
            x,y = parse_point(wkt_str)
            final_blocks.append(BlockResult(block, wkt_str))
            
        cursor.close()
        return final_blocks

class PostgisIntersectionSearcher:
    """
    Replaces the IntersectionManager clmass.
    """
    def __init__(self,conn):
        self.connection = conn

    def close(self):
        # self.connection.close()
        pass
    
    def search(self, predir_a=None, street_a=None, suffix_a=None, postdir_a=None, predir_b=None, street_b=None, suffix_b=None, postdir_b=None):
        cursor = self.connection.cursor()
        query = 'select id, pretty_name, ST_AsEWKT(location) from intersections'
        filters = []
        params = []
        if predir_a: 
            filters.append('(predir_a=%s OR predir_b=%s)')
            params.extend([predir_a, predir_a])
        if predir_b: 
            filters.append('(predir_a=%s OR predir_b=%s)')
            params.extend([predir_b, predir_b])
        if street_a: 
            filters.append('(street_a=%s OR street_b=%s)')
            params.extend([street_a, street_a])
        if street_b: 
            filters.append('(street_a=%s OR street_b=%s)')
            params.extend([street_b, street_b])
        if suffix_a: 
            filters.append('(suffix_a=%s OR suffix_b=%s)')
            params.extend([suffix_a, suffix_a])
        if suffix_b: 
            filters.append('(suffix_a=%s OR suffix_b=%s)')
            params.extend([suffix_b, suffix_b])
        if postdir_a: 
            filters.append('(postdir_a=%s OR postdir_b=%s)')
            params.extend([postdir_a, postdir_a])
        if postdir_b: 
            filters.append('(postdir_a=%s OR postdir_b=%s)')
            params.extend([postdir_b, postdir_b])
        if len(filters) > 0:
            wherestr = ' where %s' % reduce(lambda x, y: '%s and %s' % (x, y), filters)
            query += wherestr

        # This line is in IntersectionManager
        #   qs = qs.extra(select={"point": "AsText(location)"})
        # ... not sure exactly what it does here, 
        # but I'm grabbing 'location' as an WKT, so I'm assuming that this qualification 
        # doesn't matter. -TWD
        
        # TODO: can we replace these print statements with some sort of logging? 
        # print query
        # print filters

        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()

        return [IntersectionResult(res) for res in results]
