import datetime
from dateutil.relativedelta import relativedelta

from scipy import spatial

date = datetime.datetime.now()
date = date.replace(year=date.year - 2)
print(date)
