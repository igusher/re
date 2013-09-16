package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.OpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import javax.tools.FileObject;

import redis.client.RedisClient;
import redis.clients.jedis.Client;
import sun.java2d.loops.ScaledBlit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.mongodb.util.Hash;

import data.Acid;
import data.AgeGroup;
import data.Gender;
import data.Merid;
import data.Trx;

public class MainBitSet {
	static redis.client.RedisClient redClient;
	static Client redisClient; 
	static SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd");

	static Cluster cluster;
	static Session session;

	static List<Acid> acids = new ArrayList<>(550000);
	static List<Merid> merids = new ArrayList<>(7200);
	static int acidsCount = 0;

	static Calendar today = Calendar.getInstance();
	static Map<String, Integer> acidIdToArrayId = new HashMap<>();
	static Map<String, Integer> meridIdToArrayId = new HashMap<>();

	static Map<String, BitSet> inseeAcids = new HashMap<>();
	static Map<String, BitSet> meridToAcids = new HashMap<>();
	static Map<data.AgeGroup, BitSet> ageGroups = new EnumMap<>(AgeGroup.class);
	static BitSet males = null;
	static BitSet fames = null;

	static String trxSaveDir = "trx_acid_merid";
	static File acidsFile = new File("in//acids.in");
	static File trxDir = new File("in//trx");
	static File meridsFile = new File("in//merids.in");
	static File inseesFile = new File("in//insees.in");

	//
	// static File acidsFile = new File("testin//acids.in");
	// static File trxDir = new File("testin/trx");
	// static File meridsFile = new File("testin//merids.in");

	/**
	 * @param args
	 * @throws IOException
	 * @throws ParseException
	 */
	public static void main(String[] args) throws ParseException, IOException {
		// TODO Auto-generated method stub
		redClient = new RedisClient("127.0.0.1",90);
		// setUp();
		createTablesCassandra();
		saveTrxToCassandra(new Trx("356-646;05179-05179;2013-05-05;005881;1235"));
	}

	public static void setUp() throws ParseException, IOException {
		createTablesCassandra();
		insertAcids();
		readInsees();
		insertMerids();
		insertTrxs();
		System.out.println("Waiting For Requests");
		Scanner scanner = new Scanner(System.in);
		while (true) {
			try {
				String merid = scanner.nextLine(); // merid
				String listOfInsees = scanner.nextLine(); // list of insee
				String gender = scanner.nextLine(); // gender
				int ageInt = Integer.parseInt(scanner.nextLine()); // age group

				Date fromDate = sdf.parse(scanner.nextLine());
				Date toDate = sdf.parse(scanner.nextLine());
				int fromTrxsNum = Integer.parseInt(scanner.nextLine());
				int toTrxsNum = Integer.parseInt(scanner.nextLine());

				AgeGroup ageGr = null;
				switch (ageInt) {
				case 0:
					ageGr = AgeGroup.G10_25;
					break;
				case 1:
					ageGr = AgeGroup.G25_40;
					break;
				case 2:
					ageGr = AgeGroup.G40_55;
					break;
				case 3:
					ageGr = AgeGroup.G55_70;
					break;
				}
				BitSet genderBit;
				Gender g = null;
				if (gender.equals("M"))
					genderBit = BitSet.valueOf(males.toLongArray());
				// g = Gender.MALE;
				else
					genderBit = BitSet.valueOf(fames.toLongArray());
				g = Gender.FEMALE;
				String[] insees = listOfInsees.split(" ");
				BitSet inseeBit = BitSet.valueOf(inseeAcids.get(insees[0])
						.toLongArray());
				for (int i = 1; i < insees.length; i++) {
					inseeBit.or(BitSet.valueOf(inseeAcids.get(insees[i])
							.toLongArray()));
				}

				BitSet ageBit = BitSet.valueOf(ageGroups.get(ageGr)
						.toLongArray());
				BitSet meridBit = BitSet.valueOf(meridToAcids.get(merid)
						.toLongArray());

				meridBit.and(inseeBit);
				meridBit.and(genderBit);
				meridBit.and(ageBit);
				
				System.out.println(meridBit);
				int acidId = meridBit.nextSetBit(0);
				for(;acidId>=0; acidId = meridBit.nextSetBit(acidId+1))
				{
					String key = merid + "_" + acids.get(acidId).getId();
					redisClient.lrange(key, 0, -1);
					redClient.lrange(null,null,null).asStringList(Charset.defaultCharset());
				}
				
				
			} catch (Exception e) {
			}

		}
	}

	private static void createTablesCassandra() {
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect();
		session.execute("DROP TABLE simplex.trxs");
		session.execute("DROP KEYSPACE simplex");

		session.execute("CREATE KEYSPACE simplex WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor':3};");
		session.execute("CREATE TABLE simplex.trxs ("
				+ "merid_acid varchar PRIMARY KEY," + "trx_date timestamp,"
				+ "amount double," + "trx_id varchar);");
		session.execute("CREATE INDEX ind ON simplex.trxs(amount)");
	}

	private static void readInsees() throws IOException, ParseException {
		System.out.println("read INSEEs");
		FileReader fileReader = new FileReader(inseesFile);
		BufferedReader reader = new BufferedReader(fileReader);
		String insee = null;

		while ((insee = reader.readLine()) != null) {
			inseeAcids.put(insee, new BitSet(acidsCount));
		}

	}

	private static void insertAcids() throws ParseException, IOException {
		System.out.println("insert ACIDS");
		FileReader fileReader = new FileReader(acidsFile);
		BufferedReader reader = new BufferedReader(fileReader);
		String line = null;

		while ((line = reader.readLine()) != null) {
			Acid newAcid = new Acid(line);
			acids.add(newAcid);
			acidIdToArrayId.put(newAcid.getId(), acidsCount);
			acidsCount++;
		}

		males = new BitSet(acidsCount);
		fames = new BitSet(acidsCount);
		ageGroups.put(AgeGroup.G10_25, new BitSet(acidsCount));
		ageGroups.put(AgeGroup.G25_40, new BitSet(acidsCount));
		ageGroups.put(AgeGroup.G40_55, new BitSet(acidsCount));
		ageGroups.put(AgeGroup.G55_70, new BitSet(acidsCount));
		int bitId = 0;
		for (Acid acid : acids) {
			if (acid.getGender().equals(Gender.MALE))
				males.set(bitId);
			else
				fames.set(bitId);
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

	private static void insertMerids() throws IOException, ParseException {
		System.out.println("insert MERIDS");
		FileReader fileReader = new FileReader(meridsFile);
		BufferedReader reader = new BufferedReader(fileReader);
		String line = null;

		int meridsCount = 0;
		while ((line = reader.readLine()) != null) {
			Merid newMerid = new Merid(line);
			merids.add(newMerid);
			meridToAcids.put(newMerid.getId(), new BitSet(acidsCount));
			meridIdToArrayId.put(newMerid.getId(), meridsCount);
			meridsCount++;

		}

	}

	private static void insertTrxs() throws IOException, ParseException {
		for (File trxFile : trxDir.listFiles())
		// for(int i = 0 ; i < 1; i++)
		{
			Date start = new Date();
			// File trxFile = trxDir.listFiles()[i];
			System.out.println(trxFile.getAbsolutePath());
			FileReader fileReader = new FileReader(trxFile);
			BufferedReader reader = new BufferedReader(fileReader);
			String line = null;
			// List<Trx> trxs = new ArrayList<>(170000);
			while ((line = reader.readLine()) != null) {
				Trx newTrx = new Trx(line);
				int acidId = acidIdToArrayId.get(newTrx.getAcid());
				String insee = merids.get(
						meridIdToArrayId.get(newTrx.getMerid())).getInsee();
				inseeAcids.get(insee).set(acidId);
				meridToAcids.get(newTrx.getMerid()).set(acidId);
				saveTrxToFile(newTrx);
				// trxs.add(newTrx);
			}

			Date finish = new Date();
			System.out.println(start + " \t-\t" + start.getTime());
			System.out.println(finish + " \t-\t" + finish.getTime());
		}
		System.out.println("op");

	}

	private static void saveTrxToFile(Trx newTrx) throws IOException {
		File meridDir = new File(trxSaveDir + File.separator
				+ newTrx.getMerid());
		if (!meridDir.exists()) {
			meridDir.mkdir();
		}
		File meridAcidTrxs = new File(meridDir.getPath() + File.separator
				+ newTrx.getAcid());
		if (!meridAcidTrxs.exists()) {
			meridAcidTrxs.createNewFile();
		}

		FileOutputStream fos = new FileOutputStream(meridAcidTrxs);
		fos.write((newTrx.getTrxDate() + ";" + newTrx.getAmount()).getBytes());
		fos.close();
	}

	private static void saveTrxToRedis(Trx newTrx) {
		redisClient = new Client("127.0.0.1");
		String key = newTrx.getMerid() + "_" + newTrx.getAcid();
		redisClient.lpush(key.getBytes(),
				(newTrx.getTrxDate() + ";" + newTrx.getAmount()).getBytes());
		redisClient.lrange(key, 0, -1);
	}

	private static void saveTrxToCassandra(Trx newTrx) {
		// "merid_acid varchar PRIMARY KEY," +
		// "trx_date timestamp," +
		// "amount double," +
		// "trx_id int);");

		session.execute(String.format(
				"INSERT INTO simplex.trxs (merid_acid, trx_date, amount, trx_id) "
						+ "VALUES ('%s','%s',%f,'%s');", newTrx.getMerid(),
				newTrx.getTrxDate().getTime(), newTrx.getAmount(),
				newTrx.getId()));

		ResultSet rs = session.execute("SELECT * FROM simplex.trxs");
		for (Row r : rs.all()) {
			System.out.println(r.getString("merid_acid") + " "
					+ r.getDate("trx_date") + " " + r.getDouble("amount") + " "
					+ r.getString("trx_id"));
		}

		session.execute(String.format(
				"INSERT INTO simplex.trxs (merid_acid, trx_date, amount, trx_id) "
						+ "VALUES ('%s','%s',%f,'%s');", newTrx.getMerid(),
				newTrx.getTrxDate().getTime(), newTrx.getAmount() + 300,
				newTrx.getId()));

		rs = session.execute("SELECT * FROM simplex.trxs");

		// rs =
		// session.execute("SELECT *  FROM simplex.trxs WHERE merid_acid = '"+newTrx.getMerid()+"' AND amount = 5000 ");
		for (Row r : rs.all()) {
			// System.out.println(r.getLong("opa"));
			System.out.println(r.getString("merid_acid") + " "
					+ r.getDate("trx_date") + " " + r.getDouble("amount") + " "
					+ r.getString("trx_id"));
		}
	}

}
