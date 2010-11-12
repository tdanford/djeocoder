import sys
import psycopg2

import djeocoder
import postgis

# Example usage: 
#
# import postgis
# import psycopg2
# conn = psycopg2.connect('dbname=openblock user=...')
# s = postgis.PostgisBlockSearcher(conn)
# points = [(x[1], x[2]) for x in s.search('Tobin', 25)]
# s.close()

def test_PostgisBlockSearcher(cxn):
    s = postgis.PostgisBlockSearcher(cxn)
    points = [(x[1], x[2]) for x in s.search('Tobin', 25)]
    assert len(points) > 0, 'No points returned from PostgisBlockSearcher'
    print points
    s.close()

def main(argv):
    cxn = psycopg2.connect('dbname=openblock user=%s' % argv[0])
    test_PostgisBlockSearcher(cxn)
    cxn.close()

if __name__ == "__main__":
    main(sys.argv[1:])
    
