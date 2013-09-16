meridsNum = 7100;
def gen_uniformly(merids, insees):
	inseesNum = len(insees)
	density = meridsNum // inseesNum;
	
	for inseeId in range(0, inseesNum): 
		for meridId in range(0, density):
			print '{0:s};{1:s};{2:s}'.format(merids[inseeId*density + meridId],insees[inseeId],"M33")
	for meridId in range(0, meridsNum - density * inseesNum):
		print '{0};{1};{2}'.format(merids[len(merids) - 1 - meridId], insees[meridId], "K00")
	return;
def gen_merids():
	merids = list()
	for i in range(0, meridsNum):
		merids.append('{0:05}-{0:05}'.format(i));
	return merids;

def read_insees():
	file = open('./in/insees.in','r')
	insees = list()
	for line in file:
		insees.append(line.rstrip())
	return insees;

gen_uniformly(gen_merids(), read_insees());
