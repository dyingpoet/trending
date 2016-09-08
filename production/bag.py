
def from_bag(bag_str):
        if len(bag_str) <= 2:
                return list()

        bag_str = bag_str[2:-2]
        elements = bag_str.split("),(")
        results = map(lambda x : x.split(','), elements)
        return results;


def to_str(a):
    """ convert a list into str """
    return "(" + ",".join( map(str, a) ) + ")"


def to_bag(sp_vector):
    """ convert a sparse vector into bag representation in pig """
    bag_str = "{}"
    if isinstance(sp_vector, dict):
        bag_str = "{" + ",".join( [ "("+ str(k) + "," + str(v) + ")" for k, v in sp_vector.iteritems() ]) + "}"
    else:
        if isinstance(sp_vector, list) or isinstance(sp_vector, tuple):
            bag_str = "{" + ",".join( [to_str(x) for x in sp_vector] ) + "}"
        else:
            bag_str = "{" + ",".join( [to_str(x) for x in sp_vector] ) + "}"
    return bag_str



