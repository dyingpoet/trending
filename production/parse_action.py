#/usr/bin/python
import sys

sys.path.append('.')
from bag import *

for line in sys.stdin:
    words = line.strip().split('|')
    uid, bucket, visit_epoch, action_bag_str = words
    action_bag = from_bag(action_bag_str)
    #action_map = {x[0]:x[1] for x in action_bag}
    action_map = dict(action_bag)
    try:
        vid, mid = uid.split('-')
    except:
        print >> sys.stderr,  line
        raise ValueError('uid bad format %s' % (uid,))

    if vid not in ('', '\N', '#'):
        print '|'.join([vid, bucket, visit_epoch, action_map.get('item_view', '0'), action_map.get('add_to_cart', '0'), action_map.get('buyOnline', '0'), action_map.get('buyClub', '0')])
    if mid not in ('', '\N', '#'):
        print '|'.join([mid, bucket, visit_epoch, action_map.get('item_view', '0'), action_map.get('add_to_cart', '0'), action_map.get('buyOnline', '0'), action_map.get('buyClub', '0')])
        


