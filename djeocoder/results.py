
import re

point_pattern = re.compile('POINT\((-?\d+\.\d+)\s+(-?\d+\.\d+)\)')

def parse_point(wkt_str):
    matcher = point_pattern.search(wkt_str)
    if matcher==None: raise PointParsingException(wkt_str)
    x = float(matcher.group(1))
    y = float(matcher.group(2))
    return x, y

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
