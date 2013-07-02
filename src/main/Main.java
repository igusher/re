package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import dao.MySqlDao;
import data.Acid;
import data.Merid;

public class Main {

	static File acidsFile = new File("in//acids.in");
	static File trxDir = new File("in/trx");
	static File meridsFile = new File("in//merids.in");
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws ParseException, IOException {
		// TODO Auto-generated method stub
		setUp();
	}
	
	public static void setUp() throws ParseException, IOException
	{
		MySqlDao mysqlDao = new MySqlDao();
//		insertAcids(mysqlDao);
		insertMerids(mysqlDao);
	}
	private static void insertMerids(MySqlDao mysqlDao) throws IOException, ParseException {

		FileReader fileReader = new FileReader(meridsFile);
		BufferedReader reader = new BufferedReader(fileReader);
		String line = null;
		List<Merid> merids = new ArrayList<>(7200);
		while((line = reader.readLine()) != null )
		{
			merids.add(new Merid(line));
		}
		mysqlDao.storeMerids(merids);
		
	}

	private static void insertAcids(MySqlDao mysqlDao) throws ParseException, IOException
	{
		FileReader fileReader = new FileReader(acidsFile);
		BufferedReader reader = new BufferedReader(fileReader);
		String line = null;
		List<Acid> acids = new ArrayList<>(550000);
		while((line = reader.readLine()) != null )
		{
			acids.add(new Acid(line));
		}
		mysqlDao.storeAcids(acids);
	}
	
	private static void insertTrxs(MySqlDao mysqlDao) throws IOException, ParseException
	{
		for(File trxFile : trxDir.listFiles())
		{
			System.out.println(trxFile.getAbsolutePath());
			FileReader fileReader = new FileReader(trxFile);
			BufferedReader reader = new BufferedReader(fileReader);
			String line = null;
			List<Acid> acids = new ArrayList<>(170000);
			while((line = reader.readLine()) != null )
			{
				acids.add(new Acid(line));
			}
			mysqlDao.storeAcids(acids);
		}
	}

}
