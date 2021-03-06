package dao;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteResult;

import data.Acid;
import data.Merid;
import data.REQuery;
import data.Trx;

public class NewMongoDao implements IDao{
	DB mongoDb;
	DBCollection dbColl;
	DBCollection meridsColl;
	DBCollection acidsColl;
	DBCollection trxsColl;
	AtomicInteger atomicInt = new AtomicInteger();
	
	Object mock = new Object();
	Map<String, Object> merids_acids =  new ConcurrentHashMap<String,Object>();
	Map<String, Acid> acidsMap = new HashMap<String, Acid>();
	
	public NewMongoDao() throws UnknownHostException
	{
//		mongoDb.createCollection("merids", new BasicDBObject());
//		mongoDb.createCollection("acids", new BasicDBObject());
//		mongoDb.createCollection("trxs", new BasicDBObject());
		
		mongoDb = Mongo.connect(new DBAddress("127.0.0.1","27017"));
		
		meridsColl = mongoDb.getCollection("new_merids");
		acidsColl = mongoDb.getCollection("new_acids");
		trxsColl = mongoDb.getCollection("new_trxs");
		meridsColl.drop();
		acidsColl.drop();
		trxsColl.drop();
//		meridsColl.ensureIndex(new BasicDBObject().append("id", 1));
		meridsColl.ensureIndex(new BasicDBObject().append("id", 1).append("acids.id", 1));
		
		
	}
	
	public void erase(){
		meridsColl.drop();
		acidsColl.drop();
		trxsColl.drop();
	}
	
	@Override
	public void storeAcids(List<Acid> acids) {

		SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-DD");
		List<DBObject> acidDbObjects = new ArrayList<DBObject>();
		int i = 0;
		for(Acid acid : acids)
		{
			acidsMap.put(acid.getId(),acid);
			i++;
			acidDbObjects.add(new BasicDBObject("id", acid.getId()).
					append("gender", acid.getGender().toString().charAt(0)).
					append("birthDate", sdf.format(acid.getBirthDate())));
			if(i == 100000)
			{
				acidsColl.insert(acidDbObjects);
				acidDbObjects.clear();
				i=0;
			}
		}
		acidsColl.insert(acidDbObjects);
	}

	@Override
	public void storeMerids(List<Merid> merids) {
		List<DBObject> meridDbObjects = new ArrayList<DBObject>();
		for(Merid merid : merids)
		{	
			meridDbObjects.add(new BasicDBObject("id", merid.getId())
			.append("insee", merid.getInsee())
			.append("other", merid.getOther()));
			
		}
		meridsColl.insert(meridDbObjects);
		
	}

	@Override
	public int storeTrxs(List<Trx> trxs) {
		Map<String, List<Trx>> trxByMerid = new HashMap<String, List<Trx>>();
		for(Trx trx : trxs)
		{
			if(trxByMerid.containsKey(trx.getMerid()))
			{
				trxByMerid.get(trx.getMerid()).add(trx);
			}
			else
			{
				List<Trx> trxList = new ArrayList<Trx>();
				trxList.add(trx);		
				trxByMerid.put(trx.getMerid(),trxList);
			}	
		}
		
		System.out.println("Merids: " + trxByMerid.size());
		int i = 1;
		ExecutorService execService = Executors.newFixedThreadPool(10);
		for(Entry<String,List<Trx>> entry : trxByMerid.entrySet())
		{
//			System.out.println(i++);
			
			final Entry<String,List<Trx>> finalEntry = entry; 
			execService.execute(new Runnable() {
				
				@Override
				public void run() {
//					System.out.println(atomicInt.addAndGet(1));
					Map<String, Object> local_merids_acids =  new HashMap<String, Object>();
					Map<String, List<Trx>> trxByAcid = new HashMap<String, List<Trx>>();
					for(Trx trx : finalEntry.getValue())
					{
						if(trxByAcid.containsKey(trx.getAcid()))
						{
							trxByAcid.get(trx.getAcid()).add(trx);
						}
						else
						{
							List<Trx> trxList = new ArrayList<Trx>();
							trxList.add(trx);		
							trxByAcid.put(trx.getAcid(),trxList);
						}	
					}
					
					Map<String,Object> setMap = new HashMap<String,Object>();
					Map<String,Object> pushAllMap = new HashMap<String,Object>();
					
//					BasicDBObject selectAcidsOfMerid = new BasicDBObject("$match",new BasicDBObject("id",finalEntry.getKey()));
					BasicDBList acidsDBList = new BasicDBList();
					
//					List<String> existingAcids = new ArrayList<>();
//					DBCursor  c =meridsColl.find(new BasicDBObject("id", finalEntry.getKey()) , new BasicDBObject("acids", 1));
//					while(c.hasNext())
//					{
//						DBObject dbo = c.next();
//						BasicDBList acids = (BasicDBList)dbo.get("acids");
//						if(acids == null) continue;
//						for(Object a : acids)
//						{
//							DBObject ao = (DBObject)a;
//							String db = (String)ao.get("id");
//							existingAcids.add(db);
//						}
//						
//					}
					
					Map<String, BasicDBList> trxsByExistingAcid = new HashMap<String, BasicDBList>();
					for(String acidKey : trxByAcid.keySet())
					{
						Acid acid = acidsMap.get(acidKey);
						List<Trx> trxList = trxByAcid.get(acidKey);
						BasicDBList trxDbList = new BasicDBList();
						for(Trx trx :trxList)
						{
							DBObject trxDbObj = new BasicDBObject("id", trx.getId())
							.append("date", trx.getTrxDate())
							.append("amount", trx.getAmount());
							
							trxDbList.add(trxDbObj);
						}
						
						
						if(merids_acids.containsKey(finalEntry.getKey()+acidKey))
						{
							
//							System.out.println(finalEntry.getKey() + " : " + acidKey);
							trxsByExistingAcid.put(acidKey, trxDbList);
							DBObject query = new BasicDBObject("id", finalEntry.getKey()).append("acids.id", acid.getId());
							DBObject update = new BasicDBObject("$pushAll", new BasicDBObject("acids.$.trxs", trxDbList));
							try{
								WriteResult result = meridsColl.update(query, update);
//								System.out.println(result.getError());
//								System.out.println(result.getN());
							}
							catch(Exception e)
							{
								e.printStackTrace();
							}
							
						}
						else
						{
							BasicDBObject newAcid = new BasicDBObject();
							
							newAcid.append("id", acid.getId());
							newAcid.append("gender", acid.getGenderChar());
							newAcid.append("birthDate", acid.getBirthDate());
							newAcid.append("trxs", trxDbList);
							
							acidsDBList.add(newAcid);
							local_merids_acids.put(finalEntry.getKey()+acidKey, mock);
//							merids_acids.put(finalEntry.getKey()+acidKey, mock);
						}
					}
					
					BasicDBObject query = new BasicDBObject("id", finalEntry.getKey());
					DBObject update = new BasicDBObject(new BasicDBObject("$pushAll", new BasicDBObject("acids", acidsDBList)));
					merids_acids.putAll(local_merids_acids);
					try{
						WriteResult result = meridsColl.update(query, update);
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
//					System.out.println(result.getError());
//					System.out.println(result.getN());
//					
				
//					for(Trx trx : finalEntry.getValue())
//					{
//						DBObject trxDbObj = new BasicDBObject("id", trx.getId())
//						.append("date", trx.getTrxDate())
//						.append("amount", trx.getAmount());
//						
//						Acid acid = acidsMap.get(trx.getAcid());
//						if(acid == null) continue;
////						long count = meridsColl.count(new BasicDBObject("id" , trx.getMerid())
////															   .append(acid.getId(), new BasicDBObject("$exists", true)));
////						if (count == 0)
//						if(local_merids_acids.containsKey(trx.getMerid()+acid.getId()))
//						{
//						
//							BasicDBList trxList = new BasicDBList();
//							trxList.add(trxDbObj);
//							DBObject query = new BasicDBObject("id", trx.getMerid());
//							DBObject update = new BasicDBObject("$set", new BasicDBObject(acid.getId(), 
//																			new BasicDBObject("birthDate", acid.getBirthDate())
//																			.append("gender", acid.getGender().toString().charAt(0))
//																			.append("trxs", trxList)));
//							meridsColl.update(query, update);
//						
//						}
//						else
//						{
//							
//							DBObject query = new BasicDBObject("id", trx.getMerid());
//							DBObject update = new BasicDBObject("$push", new BasicDBObject( acid.getId()+".trxs", trxDbObj));
//							
//							meridsColl.update(query, update);
//						}
//						
//						local_merids_acids.put(trx.getMerid()+acid.getId(), mock);

//					}
				}
			});
		}
		
		execService.shutdown();
		try {
			execService.awaitTermination(10, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		
		return 10; //TODO: return number of stored trxs
	
	}

	public int getAcidsNum(REQuery reQuery) {
		Calendar cal = Calendar.getInstance();
//		cal.add(Calendar.YEAR, - reQuery.getMinAge());
		cal.add(Calendar.YEAR, - 10); //TODO: hardfix
		Date gBirthDate = cal.getTime();
		cal = Calendar.getInstance();
//		cal.add(Calendar.YEAR, - reQuery.getMaxAge());
		cal.add(Calendar.YEAR, - 25); //TODO: hardfix 
		Date lBirthDate = cal.getTime();
		DBObject matchMerid = new BasicDBObject("$match", new BasicDBObject("id", reQuery.getMerid()));
		DBObject unwindAcids = new BasicDBObject("$unwind", "$acids");
		DBObject matchAcids = new BasicDBObject("$match", new BasicDBObject("acids.gender", reQuery.getGenderChar()).append("acids.birthDate", new BasicDBObject("$gte", lBirthDate).append("$lte", gBirthDate)));
		DBObject unwindTrxs = new BasicDBObject("$unwind", "$acids.trxs");
		DBObject matchTrxs = new BasicDBObject("$match", new BasicDBObject("acids.trxs.date", new BasicDBObject("$gte",reQuery.getFromDate()).append("$lte", reQuery.getToDate())));
		DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$acids.id").append("trx_count", new BasicDBObject("$sum",1)));
		DBObject matchTrxsCount = new BasicDBObject("$match", new BasicDBObject("trx_count", new BasicDBObject("$gte",reQuery.getMinTrxNum()).append("$lte", reQuery.getMaxTrxNum())));
		DBObject countAcids = new BasicDBObject("$group", new BasicDBObject("_id", null).append("count", new BasicDBObject("$sum",1)));
		AggregationOutput aggrOut = meridsColl.aggregate(matchMerid, unwindAcids,matchAcids, unwindTrxs, matchTrxs, group, matchTrxsCount, countAcids);
		System.out.println(aggrOut);
		
//		DBObject groupFields = new BasicDBObject("acid", "444-256");
//		groupFields.put("count", new BasicDBObject( "$sum", "1"));
//		
//		AggregationOutput aggrOut = trxsColl.aggregate(new BasicDBObject("$group", groupFields));
//		for(DBObject dbo : aggrOut.results())
//		{
//			System.out.println(dbo);
//		}
		return 3;
	}

	@Override
	public boolean storeTrx(Trx trx) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void storeInsees(List<String> insees) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void storeAcid(Acid acid) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void storeMerid(Merid merid) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void storeInsee(String insee) {
		// TODO Auto-generated method stub
		
	}

}
