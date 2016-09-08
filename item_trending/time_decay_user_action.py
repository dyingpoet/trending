import fileinput
import math
import re
import string
import sys
from collections import defaultdict
from datetime import datetime

def day_diff(d1, d2):
    d1 = datetime.strptime(d1, "%Y%m%d")
    d2 = datetime.strptime(d2, "%Y%m%d")
    return abs((d2 - d1).days)

def main():
    for line in fileinput.input('-'):
        line = line.rstrip()
        vid, mid, action_type,  catalog_item_id,  bucket,  visit_date, now_date, lmda, weight1, weight2, weight3 = line.split("\t")
        t = day_diff(visit_date, now_date)
        score = 0.0

        if action_type == 'item_view':
            score = float(weight1) * math.exp(-1.0 * float(lmda) * float(t))
        elif action_type == 'add_to_cart':
            score = float(weight2) * math.exp(-1.0 * float(lmda) * float(t))
        elif action_type == 'order':
            score = float(weight3) * math.exp(-1.0 * float(lmda) * float(t))
        else:
            score = 0.0

        print str(mid) + "\t" + str(bucket)+ "\t"+ str(score)
main()
