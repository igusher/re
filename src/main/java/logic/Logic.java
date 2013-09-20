package logic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import dao.IDao;
import dao.RedisDao;
import data.Acid;
import data.Merid;
import data.REQuery;
import data.Trx;

public class Logic implements ILogic {
	// Test Sources
	// static File acidsFile = new File("testin//acids.in");
	// static File trxDir = new File("testin/trx");
	// static File meridsFile = new File("testin//merids.in");
	
	final File acidsFile = new File("in//acids.in");
	final File trxDir = new File("in//trx");
	final File meridsFile = new File("in//merids.in");
	final File inseesFile = new File("in//insees.in");

	final String host = "127.0.0.1";
	final int port = 8319;
	IDao dao;

	@Override
	public void setUp() throws ParseException, IOException {
		dao = new RedisDao(host, port);

		setUpAcids();
		setUpInsees();
		setUpMerids();
//		setUpTrxs();
	}
	
	private void setUpAcids() throws ParseException, IOException {
		List<Acid> acids = readAcids();
		dao.storeAcids(acids);
	}
	
	private void setUpInsees() throws IOException, ParseException {
		List<String> insees = readInsees();
		dao.storeInsees(insees);
	}
	
	private void setUpMerids() throws IOException, ParseException {
		List<Merid> merids = readMerids();
		dao.storeMerids(merids);
	}
	
	private void setUpTrxs() throws IOException, ParseException {
		for (File trxFile : trxDir.listFiles()) {
			// for (int i = 0; i < 1; i++) {
			Date start = new Date();
			// File trxFile = trxDir.listFiles()[i];
			setUpTrxFromFile(trxFile);

			Date finish = new Date();
			System.out.println(start + " \t-\t" + start.getTime());
			System.out.println(finish + " \t-\t" + finish.getTime());
		}
	}

	private void setUpTrxFromFile(File trxFile) throws IOException,
			ParseException {
		List<Trx> trxs = readTrxs(trxFile);
		dao.storeTrxs(trxs);
	}

	private List<Acid> readAcids() throws ParseException, IOException {
		System.out.println("insert ACIDS");
		System.out.println(acidsFile.getAbsolutePath());
		FileReader fileReader = new FileReader(acidsFile);
		BufferedReader reader = new BufferedReader(fileReader);
		String acidString = null;
		List<Acid> acids = new ArrayList<Acid>(550000);
		while ((acidString = reader.readLine()) != null) {
			Acid newAcid = new Acid(acidString);
			acids.add(newAcid);
		}

		return acids;
	}

	private List<String> readInsees() throws IOException, ParseException {
		System.out.println("read INSEEs");
		System.out.println(inseesFile.getAbsolutePath());
		FileReader fileReader = new FileReader(inseesFile);
		BufferedReader reader = new BufferedReader(fileReader);
		String insee = null;
		List<String> insees = new ArrayList<String>();
		while ((insee = reader.readLine()) != null) {
			insees.add(insee);
		}
		return insees;
	}

	private List<Merid> readMerids() throws IOException, ParseException {
		System.out.println("insert MERIDS");
		System.out.println(meridsFile.getAbsolutePath());
		FileReader fileReader = new FileReader(meridsFile);
		BufferedReader reader = new BufferedReader(fileReader);
		String meridString = null;
		List<Merid> merids = new ArrayList<Merid>();

		while ((meridString = reader.readLine()) != null) {
			Merid newMerid = new Merid(meridString);
			merids.add(newMerid);
		}
		return merids;

	}
	

	private List<Trx> readTrxs(File trxFile) throws IOException, ParseException {
		System.out.println(trxFile.getAbsolutePath());
		FileReader fileReader = new FileReader(trxFile);
		BufferedReader reader = new BufferedReader(fileReader);
		String trxString = null;
		List<Trx> trxs = new ArrayList<Trx>(170000);
		while ((trxString = reader.readLine()) != null)
			trxs.add(new Trx(trxString));
		return trxs;
	}

	@Override
	public int getAcidsNum(REQuery reQuery) {
		return dao.getAcidsNum(reQuery);
	}

	@Override
	public int submitTrxsAsTextBlock(String trxsBlock) {
		int storedCount = 0;
		BufferedReader reader = new BufferedReader(new StringReader(trxsBlock));
		String trxString = null;
		try{
		while ((trxString = reader.readLine()) != null)
			if (dao.storeTrx(new Trx(trxString)))
				storedCount++;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return 0;
		}
		
		return storedCount;
	}

}
