from datetime import date
from datetime import timedelta
sameBirthDate = 150
acidsNum = 500000
sex = ['M','F']
startDate = date(1980,1,1)
birthDate = []
for i in range(0,365*5):
	d = timedelta(days=i)
	birthDate.append(startDate + d)
for i in range (0, acidsNum):
	i_str = '{0:06d}'.format(i)
	outstr = i_str[:3] + '-' + i_str[3:] + ';'
	outstr += sex[i%2] + ';'
	outstr += birthDate[i%(365*5)].isoformat();
	print outstr
