package dao;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import data.Gender;
import data.REQuery;
import data.Trx;

public class NewMongoDaoTest {

	IDao dao;
	REQuery req;
	
	@Before
	public void setUp() throws UnknownHostException, ParseException
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
//		dao = new NewMongoDao();
//		req = new REQuery(Gender.MALE, "04081-04081",);
//		req.setGender(Gender.MALE);
//		req.setMerid("04081-04081");
//		req.setMinTrxNum(0);
//		req.setMaxTrxNum(5);
//		req.setFromDate(sdf.parse("2013-05-02"));
//		req.setToDate(sdf.parse("2013-06-29"));
//		req.setMinAge(25);
//		req.setMaxAge(50);
	}
	
	
	@Ignore
	@Test
	public void getAcidsNumTest() {
		Date start = new Date();
		dao.getAcidsNum(req);
		Date finish = new Date();
		System.out.println(start + " \t-\t" + start.getTime());
		System.out.println(finish + " \t-\t" + finish.getTime());
		
	}
	

}
