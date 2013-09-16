import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.Set;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;


public class Main {

	static DB mongoDb;
	static DBCollection dbColl;
	static DBCollection meridsColl;
	static DBCollection acidsColl;
	static DBCollection trxsColl;
	/**
	 * @param args
	 * @throws UnknownHostException 
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws UnknownHostException, ParseException {
		
		
	
			mongoDb = Mongo.connect(new DBAddress("127.0.0.1","27017"));
			dbColl = mongoDb.getCollection("test");
//			mongoDb.createCollection("merids", new BasicDBObject());
//			mongoDb.createCollection("acids", new BasicDBObject());
//			mongoDb.createCollection("trxs", new BasicDBObject());
			
			meridsColl = mongoDb.getCollection("new_merids");
			acidsColl = mongoDb.getCollection("acids");
			trxsColl = mongoDb.getCollection("trxs");
			
			System.out.println(meridsColl.count());
			System.out.println(acidsColl.count());
			System.out.println(trxsColl.count());
			
			System.out.println(trxsColl.findOne());
//			countTrxsNum(meridsColl);
			countAcidNum(meridsColl);
//			DBCursor  c =meridsColl.find();
//			while(c.hasNext())
//			{
//				System.out.println(c.next());
//			}
//			insertSome(null);
	}

	private static void insertSome(DBCollection coll) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date startDate = sdf.parse("2013-05-03");
		Date endDate = sdf.parse("2013-05-15");
		DBObject match = new BasicDBObject("$match", new BasicDBObject("date", new BasicDBObject("$gte", startDate).append("$lte", endDate)));
		
		DBObject fields = new BasicDBObject("date", 1).append("amount", 1).append("acid", 1);
		DBObject project = new BasicDBObject("$project", fields );

		
		DBObject groupFields = new BasicDBObject( "_id", "$acid");
		groupFields.put("count", new BasicDBObject( "$sum", 1));
//		groupFields.put("trxDate", "$date");
		
		DBObject group =  new BasicDBObject("$group", groupFields);
		
		DBObject match2 = new BasicDBObject("$match", new BasicDBObject("count", new BasicDBObject("$gte", 5).append("$lte", 5)));
		
		Date start = new Date();
		AggregationOutput aggrOut = trxsColl.aggregate(match,project, group,match2);
		Date finish = new Date();
		System.out.println(start + " \t-\t" + start.getTime());
		System.out.println(finish + " \t-\t" + finish.getTime());
//		System.out.println(aggrOut.results().iterator().);
		int sum = 0;
		for(DBObject dbo : aggrOut.results())
		{
			sum++;
//			System.out.println(dbo);
		}
		System.out.println(sum);
	}


	
	public static void countTrxsNum(DBCollection coll)
	{
		AggregationOutput aggrOut = coll.aggregate(new BasicDBObject("$unwind", "$acids"), new BasicDBObject("$unwind", "$acids.trxs"), new BasicDBObject("$group", new BasicDBObject("_id", null).append("count", new BasicDBObject("$sum", 1))));
		System.out.println(aggrOut);
	}
	
	public static void countAcidNum(DBCollection coll)
	{
		Date start = new Date();
		AggregationOutput aggrOut = coll.aggregate(new BasicDBObject("$unwind", "$acids"), new BasicDBObject("$group", new BasicDBObject("_id", "$acids.id")), 
					new BasicDBObject("$group", new BasicDBObject("_id", null).append("count", new BasicDBObject("$sum", 1))));
		
//		AggregationOutput aggrOut = coll.aggregate(new BasicDBObject("$unwind", "$acids"));
		System.out.println(aggrOut);
		Date finish = new Date();
		System.out.println(start + " \t-\t" + start.getTime());
		System.out.println(finish + " \t-\t" + finish.getTime());
//		System.out.println(aggrOut.results().iterator().);
		
	}

}
