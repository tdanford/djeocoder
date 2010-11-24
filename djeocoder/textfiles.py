import re
import string
import gzip

def line_generator(inf):
	line = inf.readline()
	while line != None and len(line) > 0:
		yield line
		line = inf.readline()

class PipeFileLoader(object):
    def __init__(self, filename):
		if filename.endswith('.gz'):
			inf = gzip.open(filename, 'r')
		else:
			inf = open(filename, 'r')
        self.rows = []
        self.column_names = []
        self.columns = {}
		for line in line_generator(inf):
            self.rows.append([x.strip() for x in line.split('|')])
        inf.close()
    def row_as_dict(self, row):
        d = {}
        for i in range(len(self.column_names)):
            d[self.column_names[i]] = row[i]
        return d
    def scan(self):
        for x in self.rows:
            yield self.row_as_dict(x)
    def dict_as_tuple(self, dict):
        lst = []
        for k in self.column_names:
            if k in dict.keys():
                lst.append(dict[k])
        return tuple(lst)
    def select(self, **kwargs):
        return list(self.tuples(matches(self.scan(), **kwargs)))
    def tuples(self, selector):
        for x in selector:
            yield self.dict_as_tuple(x)

def list_selector(lst):
    for x in lst:
        yield x

def value_or_none(d, key):
    try: return d[key]
    except KeyError: None

def concat(d1, d2, p1='', p2=''):
    dplus = {}
    for k in d1.keys():
        knew = p1 + str(k)
        dplus[knew] = d1[k]
    for k in d2.keys():
        knew = p2 + str(k)
        dplus[knew] = d2[k]
    return dplus

def inner_join(selector1, selector2, key, prefix1='', prefix2=''):
    list2 = list(selector2)
    for x1 in selector1:
        v1 = value_or_none(x1, key)
        for x2 in list2:
            v2 = value_or_none(x2, key)
            if v1==v2:
                yield concat(x1, x2, prefix1, prefix2)

def dict_matches_predicates(dict, *predlist):
    for p in predlist:
        if not p(dict):
            return False
    return True

def dict_as_tuple(d):
    return tuple(sorted(d.items()))

def where(selector, *predlist):
    for x in selector:
        if dict_matches_predicate(x, *predlist):
            yield x

def KEYEQ(k, v):
    return lambda dict: k in dict.keys() and dict[k] == v

def KEYNEQ(k, v):
    return lambda dict: (not (k in dict.keys())) or dict[k] != v

def AND(*predlist):
    return lambda d : apply_predlist_and(d, predlist)

def OR(*predlist):
    return lambda d : apply_predlist_or(d, predlist)

def NOT(p):
    return lambda d : not p(d)

def apply_predlist_and(d, predlist):
    for p in predlist:
        if not p(d):
            return False
    return True

def apply_predlist_or(d, predlist):
    for p in predlist:
        if p(d):
            return True
    return False

def matches(selector, **kwargs):
    preds = [KEYEQ(k, kwargs[k]) for k in kwargs.keys()]
    return where(selector, preds)

def select(selector, *ks):
    for x in selector:
        d = {}
        for k in ks:
            if k in x: d[k] = x[k]
            else: d[k] = None
        yield d

def count(selector):
    c = 0
    for x in selector: c += 1
    return c

def group_by(selector, key):
    groups = {}
    for x in selector:
        value = value_or_none(x, key)
        try: groups[value].append(x)
        except KeyError: groups[value] = [x]
    selectors = {}
    for key in groups.keys():
        selectors[key] = list_selector(groups[key])
    return selectors

def limit(selector, n):
    i = 0
    for x in selector:
        if i < n:
            i += 1
            yield x
        else:
            break

def aggregate(aggregator, key, selector):
    sels = group_by(selector, key)
    for key in sels.keys():
        yield aggregator(sels[key])

def distinct(selector):
    seen = {}
    for x in selector:
        t = dict_as_tuple(x)
        if not (t in seen.keys()):
            seen[t] = x
            yield x

def first(selector):
    yield selector.next()

class IntersectionFileLoader(PipeFileLoader):
    def __init__(self, filename):
        PipeFileLoader.__init__(self, filename)
        self.column_names = ['id', 'pretty_name', 'slug', 'predir_a', 'street_a', 'suffix_a', 'postdir_a', 'predir_b', 'street_b', 'suffix_b', 'postdir_b', 'zip', 'city', 'state', 'location' ]
        for i in range(len(self.column_names)):
            self.columns[self.column_names[i]] = i

class BlockFileLoader(PipeFileLoader):
    def __init__(self, filename):
        PipeFileLoader.__init__(self, filename)
        self.column_names = ['id', 'pretty_name', 'predir', 'street', 'street_slug', 'street_pretty_name', 'suffix', 'postdir', 'left_from_num', 'left_to_num', 'right_from_num', 'right_to_num', 'from_num', 'to_num', 'left_zip', 'right_zip', 'left_city', 'right_city', 'left_state', 'right_state', 'parent_id', 'geom']
        for i in range(len(self.column_names)):
            self.columns[self.column_names[i]] = i
    def select(self):
        query = 'select id, pretty_name, from_num, to_num, left_from_num, left_to_num, right_from_num, right_to_num, ST_AsEWKT(geom) from blocks where street=%s' 

