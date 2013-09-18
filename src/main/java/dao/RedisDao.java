package dao;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;

import data.Acid;
import data.AgeGroup;
import data.Gender;
import data.Merid;
import data.REQuery;
import data.Trx;

public class RedisDao implements IDao {

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	Jedis jedisClient;

	List<Acid> acids = new ArrayList<Acid>(550000);
	List<Merid> merids = new ArrayList<Merid>(7200);
	int acidsCount = 0;

	Calendar today = Calendar.getInstance();
	Map<String, Integer> acidIdToArrayId = new HashMap<String, Integer>();
	Map<String, Integer> meridIdToArrayId = new HashMap<String, Integer>();

	Map<String, BitSet> inseeAcids = new HashMap<String, BitSet>();
	Map<String, BitSet> meridToAcids = new HashMap<String, BitSet>();
	Map<data.AgeGroup, BitSet> ageGroups = new EnumMap<AgeGroup, BitSet>(
			AgeGroup.class);
	BitSet males = null;
	BitSet fames = null;

	public RedisDao(String host, int port) {
		jedisClient = new Jedis(host);
	}

	@Override
	public void erase() {
		jedisClient.flushDB();
		acids.clear();
		merids.clear();
		acidIdToArrayId.clear();
		meridIdToArrayId.clear();
		inseeAcids.clear();
		meridToAcids.clear();
		ageGroups.clear();
		males.clear();
		fames.clear();
	}

	@Override
	public void storeAcids(List<Acid> acids) {
		acidsCount = acids.size();
		this.acids = acids;
		males = new BitSet(acidsCount);
		fames = new BitSet(acidsCount);
		ageGroups.put(AgeGroup.G10_25, new BitSet(acidsCount));
		ageGroups.put(AgeGroup.G25_40, new BitSet(acidsCount));
		ageGroups.put(AgeGroup.G40_55, new BitSet(acidsCount));
		ageGroups.put(AgeGroup.G55_70, new BitSet(acidsCount));
		int acidArrayId = 0;
		int bitId = 0;
		for (Acid acid : acids) {
			acidIdToArrayId.put(acid.getId(), acidArrayId);

			if (acid.getGender().equals(Gender.MALE))
				males.set(bitId);
			else
				fames.set(bitId);
			// TODO: Change operations with dates on not deprecated
			int years = today.get(Calendar.YEAR)
					- acid.getBirthDate().getYear() - 1900;
			if ((years >= 10) && (years < 25))
				ageGroups.get(AgeGroup.G10_25).set(bitId);
			if ((years > 25) && (years < 40))
				ageGroups.get(AgeGroup.G25_40).set(bitId);
			if ((years >= 40) && (years < 55))
				ageGroups.get(AgeGroup.G40_55).set(bitId);
			if ((years >= 55) && (years < 70))
				ageGroups.get(AgeGroup.G55_70).set(bitId);
			bitId++;
		}
	}

	@Override
	public void storeMerids(List<Merid> merids) {
		this.merids = merids;
		int meridArrayId = 0;
		for (Merid merid : merids) {
			meridToAcids.put(merid.getId(), new BitSet(acidsCount));
			meridIdToArrayId.put(merid.getId(), meridArrayId);
		}

	}

	@Override
	public int storeTrxs(List<Trx> trxs) {
		int storedTrxsCount = 0;
		for (Trx trx : trxs) 
			if (storeTrx(trx))
				storedTrxsCount++;
		return storedTrxsCount;
	}
	
	@Override
	public boolean storeTrx(Trx trx) {
		Integer acidId = acidIdToArrayId.get(trx.getAcid());
		if (acidId == null)
			return false;
		Integer meridId = meridIdToArrayId.get(trx.getMerid());
		if (meridId == null)
			return false;

		String insee = merids.get(meridIdToArrayId.get(trx.getMerid()))
				.getInsee();
		inseeAcids.get(insee).set(acidId);
		meridToAcids.get(trx.getMerid()).set(acidId);

		saveTrxToRedis(trx);

		return true;
	}

	private void saveTrxToRedis(Trx newTrx) {

		String key = newTrx.getMerid() + "_" + newTrx.getAcid();
		jedisClient.lpush(key.getBytes(), (sdf.format(newTrx.getTrxDate())
				+ ";" + newTrx.getAmount()).getBytes());
	}

	@Override
	public void storeInsees(List<String> insees) {
		for (String insee : insees)
			inseeAcids.put(insee, new BitSet(acidsCount));
	}

	@Override
	public int getAcidsNum(REQuery reQuery) {
		int resultAcidNum = 0;
		List<String> insees = reQuery.getInsees();
		BitSet inseeBit = BitSet.valueOf(inseeAcids.get(insees.get(0))
				.toLongArray());
		for (int i = 1; i < insees.size(); i++) {
			inseeBit.or(BitSet.valueOf(inseeAcids.get(insees.get(i))
					.toLongArray()));
		}

		BitSet ageBit = BitSet.valueOf(ageGroups.get(reQuery.getAgeGroup())
				.toLongArray());
		BitSet meridBit = BitSet.valueOf(meridToAcids.get(reQuery.getMerid())
				.toLongArray());

		BitSet genderBitSet = null;
		Gender gender = reQuery.getGender();
		if (gender.equals(Gender.MALE))
			genderBitSet = BitSet.valueOf(males.toLongArray());
		else
			genderBitSet = BitSet.valueOf(fames.toLongArray());

		meridBit.and(inseeBit);
		meridBit.and(genderBitSet);
		meridBit.and(ageBit);

		System.out.println(meridBit);
		int acidId = meridBit.nextSetBit(0);
		for (; acidId >= 0; acidId = meridBit.nextSetBit(acidId + 1)) {
			String key = reQuery.getMerid() + "_" + acids.get(acidId).getId();
			jedisClient.lrange(key, 0, -1);
			List<String> trxsStrings = jedisClient.lrange(key, 0, -1);
			System.out.println(key);
			int count = 0;

			for (String trx : trxsStrings) {
				Date trxDate = null;
				try {
					trxDate = sdf.parse(trx.split(";")[0]);
				} catch (Exception e) {
					e.printStackTrace();
					break;
				}
				if ((trxDate.after(reQuery.getFromDate()))
						&& (trxDate.before(reQuery.getToDate())))
					count++;
			}
			if ((count >= reQuery.getMinTrxNum())
					&& (count <= reQuery.getMaxTrxNum())) {
				resultAcidNum++;
			}
		}
		return resultAcidNum;
	}



}
