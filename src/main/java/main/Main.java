package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import dao.IDao;
import dao.InMemDao;
import dao.NewMongoDao;
import data.Acid;
import data.Merid;
import data.Trx;

public class Main {
	
	

	static File acidsFile = new File("in//acids.in");
	static File trxDir = new File("in/trx");
	static File meridsFile = new File("in//merids.in");
//	
//	static File acidsFile = new File("testin//acids.in");
//	static File trxDir = new File("testin/trx");
//	static File meridsFile = new File("testin//merids.in");
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main1(String[] args) throws ParseException, IOException {
		// TODO Auto-generated method stub
//		setUp();
	}
	public static void main(String[] args) throws ParseException, IOException {
		// TODO Auto-generated method stub
//		setUp();
		InMemDao inMemDao = new InMemDao(trxDir);
		
		System.out.println("Read data: ...");
		Date start = new Date();
		inMemDao. readData();
		Date finish = new Date();
		System.out.println(start + " \t-\t" + start.getTime());
		System.out.println(finish + " \t-\t" + finish.getTime());
		
		System.out.println("Query data: ...");
		start = new Date();
		Map<Integer, AtomicInteger> map = inMemDao.queryData();
		finish = new Date();
		System.out.println(start + " \t-\t" + start.getTime());
		System.out.println(finish + " \t-\t" + finish.getTime());
		
		System.out.println("Check data: ...");
		start = new Date();
		int sum  = 0;
		for(Entry<Integer,AtomicInteger> entry : map.entrySet())
		{
			sum = sum + entry.getValue().get();
		}
		System.out.println(sum);
		finish = new Date();
		System.out.println(start + " \t-\t" + start.getTime());
		System.out.println(finish + " \t-\t" + finish.getTime());
		
	}
	

	public static void setUp() throws ParseException, IOException
	{
//		IDao dao = new MySqlDao();
//		IDao dao  = new MongoDao();
		IDao dao  = new NewMongoDao();
		dao.erase();
		insertAcids(dao);
		insertMerids(dao);
		insertTrxs(dao);
		
	}
	private static void insertMerids(IDao dao) throws IOException, ParseException {
		System.out.println("insert MERIDS");
		FileReader fileReader = new FileReader(meridsFile);
		BufferedReader reader = new BufferedReader(fileReader);
		String line = null;
		List<Merid> merids = new ArrayList<Merid>(7200);
		while((line = reader.readLine()) != null )
		{
			merids.add(new Merid(line));
		}
		dao.storeMerids(merids);
		
	}

	private static void insertAcids(IDao dao) throws ParseException, IOException
	{
		System.out.println("insert ACIDS");
		FileReader fileReader = new FileReader(acidsFile);
		BufferedReader reader = new BufferedReader(fileReader);
		String line = null;
		List<Acid> acids = new ArrayList<Acid>(550000);
		while((line = reader.readLine()) != null )
		{
			acids.add(new Acid(line));
		}
		dao.storeAcids(acids);
	}
	
	private static void insertTrxs(IDao dao) throws IOException, ParseException
	{

		
		for(File trxFile : trxDir.listFiles())
//		for(int i = 0 ; i < 10; i++)		
		{
			Date start = new Date();
//			File trxFile = trxDir.listFiles()[i];
			System.out.println(trxFile.getAbsolutePath());
			FileReader fileReader = new FileReader(trxFile);
			BufferedReader reader = new BufferedReader(fileReader);
			String line = null;
			List<Trx> trxs = new ArrayList<Trx>(170000);
			while((line = reader.readLine()) != null )
			{
				trxs.add(new Trx(line));
			}
			dao.storeTrxs(trxs);
			Date finish = new Date();
			System.out.println(start + " \t-\t" + start.getTime());
			System.out.println(finish + " \t-\t" + finish.getTime());
		}

		
	}

}
