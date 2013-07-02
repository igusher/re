package dao;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.List;

import javax.print.attribute.standard.MediaSize.ISO;

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
import data.Trx;

public class Dao implements IDao{
	DB mongoDb;
	DBCollection dbColl;
	DBCollection acidsCollection;
	
	Dao() throws UnknownHostException
	{
		mongoDb = Mongo.connect(new DBAddress("127.0.0.1","27017"));
		dbColl = mongoDb.getCollection("test");
	}
	
	@Override
	public void storeAcids(List<Acid> acids) {

		SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-DD");
		for(Acid acid : acids)
		{
			dbColl.insert(new BasicDBObject("id", acid.getId()).
					append("gender", acid.getGender().toString().charAt(0)).
					append("birthDate", sdf.format(acid.getBirthDate()))); 
		}
		
	}

	@Override
	public void storeMerids(List<Merid> merids) {
		for(Merid merid : merids)
		{
//			dbColl 
		}
		
	}

	@Override
	public void storeTrx(List<Trx> trxs) {
		for(Trx trx : trxs)
		{
			
		}
		
	}


}
