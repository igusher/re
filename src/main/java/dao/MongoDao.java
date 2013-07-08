package dao;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.print.attribute.standard.MediaSize.ISO;
import javax.swing.plaf.basic.BasicScrollBarUI;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import data.Acid;
import data.Gender;
import data.Merid;
import data.REQuery;
import data.Trx;

public class MongoDao implements IDao{
	DB mongoDb;
	DBCollection dbColl;
	DBCollection meridsColl;
	DBCollection acidsColl;
	DBCollection trxsColl;
	
	public MongoDao() throws UnknownHostException
	{
		mongoDb = Mongo.connect(new DBAddress("127.0.0.1","27017"));
		dbColl = mongoDb.getCollection("test");
//		mongoDb.createCollection("merids", new BasicDBObject());
//		mongoDb.createCollection("acids", new BasicDBObject());
//		mongoDb.createCollection("trxs", new BasicDBObject());
		
		meridsColl = mongoDb.getCollection("merids");
		acidsColl = mongoDb.getCollection("acids");
		trxsColl = mongoDb.getCollection("trxs");
		
		meridsColl.drop();
		acidsColl.drop();
		trxsColl.drop();
	}
	
	@Override
	public void storeAcids(List<Acid> acids) {

		SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-DD");
		List<DBObject> acidDbObjects = new ArrayList<>();
		int i = 0;
		for(Acid acid : acids)
		{
			i++;
			acidDbObjects.add(new BasicDBObject("id", acid.getId()).
					append("gender", acid.getGender().toString().charAt(0)).
					append("birthDate", sdf.format(acid.getBirthDate())));
			if(i == 10000)
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
		List<DBObject> meridDbObjects = new ArrayList<>();
		for(Merid merid : merids)
		{	
			meridDbObjects.add(new BasicDBObject("id", merid.getId())
			.append("insee", merid.getInsee())
			.append("other", merid.getOther()));
			
		}
		meridsColl.insert(meridDbObjects);
		
	}

	@Override
	public void storeTrx(List<Trx> trxs) {
		List<DBObject> trxDbObjects = new ArrayList<>();
		for(Trx trx : trxs)
		{
			trxDbObjects.add(new BasicDBObject("id", trx.getId())
			.append("acid", trx.getAcid())
			.append("merid", trx.getMerid())
			.append("date", trx.getTrxDate())
			.append("amount", trx.getAmount()));
		}
		trxsColl.insert(trxDbObjects);
		
	}

	public int getAcidsNum(REQuery reQuery) {
		
		DBObject groupFields = new BasicDBObject( "acid", "444-256");
		groupFields.put("count", new BasicDBObject( "$sum", "1"));
		
		AggregationOutput aggrOut = trxsColl.aggregate(new BasicDBObject("$group", groupFields));
		for(DBObject dbo : aggrOut.results())
		{
			System.out.println(dbo);
		}
		return 3;
	}

	@Override
	public void erase() {
		// TODO Auto-generated method stub
		
	}
}
