import random
from datetime import date
from datetime import timedelta

numOfTrxPerDay = 150000;

def read_merids():
	file = open("in/merids.in",'r')
	merids = []
	for line in file:
		merids.append(line.split(';')[0])
	return merids;

def read_acids():
	file = open("in/acids.in",'r')
	acids = []
	for line in file:
		acids.append(line.split(';')[0])
	return acids;

startDate = date.today() - timedelta(days=60)
trxDates = [startDate + timedelta(x) for x in range(0,60)]

trxids = [x for x in range(0, numOfTrxPerDay)]
merids = read_merids()
acids = read_acids()


for curDate in trxDates:
	print curDate
	outfile = open("trx"+curDate.isoformat()+".in","w");
	for trxId in trxids:
		acidId = random.randint(0, len(acids)-1)
		merId = random.randint(0, len(merids)-1)
		amount = random.randint(0, 2000)
		trxstr = '{0};{1};{2};{3:06d};{4}\n'.format(acids[acidId], merids[merId],curDate.isoformat(),trxId,amount)
		outfile.write(trxstr);



