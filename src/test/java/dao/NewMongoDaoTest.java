package dao;

import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.Before;
import org.junit.Test;

import data.Gender;
import data.REQuery;

public class NewMongoDaoTest {

	IDao dao;
	REQuery req;
	
	@Before
	public void setUp() throws UnknownHostException, ParseException
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		dao = new NewMongoDao();
		req = new REQuery();
		req.setGender(Gender.MALE);
		req.setMerid("04381-04381");
		req.setMinTrxNum(1);
		req.setMaxTrxNum(3);
		req.setFromDate(sdf.parse("2013-05-02"));
		req.setToDate(sdf.parse("2013-05-09"));
		req.setMinAge(25);
		req.setMaxAge(32);
	}
	@Test
	public void getAcidsNumTest() {
		dao.getAcidsNum(req);
	}
	
	

}
