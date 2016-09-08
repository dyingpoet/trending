import fileinput
import math
import re
import string
import sys
from collections import defaultdict
from datetime import datetime

def day_diff(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)

def main():
    for line in fileinput.input('-'):
        line = line.rstrip()
        system_item_nbr, catalog_item_id, day_sum_qty, day_sum_retail, day_sum_visit, visit_date, current_date, lmda1, lmda2, lmda3 = line.split("\t")
        t = day_diff(visit_date, current_date)
        decay_qty = float(day_sum_qty) * math.exp(-1.0 * float(lmda1) * float(t))
        decay_retail = float(day_sum_retail) * math.exp(-1.0 * float(lmda2) * float(t))
        decay_visit = float(day_sum_visit) * math.exp(-1.0 * float(lmda3) * float(t))
        print str(system_item_nbr) + "\t" + str(catalog_item_id) + "\t" + str(day_sum_qty)+ "\t"+ str(decay_qty) + "\t" + str(day_sum_retail) + "\t" + str(decay_retail) + "\t" + str(day_sum_visit )+ "\t" + str(decay_visit) + "\t" + visit_date
main()
