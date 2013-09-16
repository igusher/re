package dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


import data.Acid;
import data.Merid;
import data.REQuery;
import data.Trx;

public class InMemDao{
	public SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	AtomicInteger acidsCount = new AtomicInteger(0);
	final Map<Integer,AtomicInteger> trxByAcidsCount = new ConcurrentHashMap<>();
	
	File trxDir;
	List<TrxTuple> trxsList = (List<TrxTuple>) Collections.synchronizedList(new ArrayList<TrxTuple>(9000000));
	
	public InMemDao(File trxDir)
	{
		this.trxDir = trxDir;
	}
	
	public  void readData() {
		List<Thread> threads = new ArrayList<>();
		for(File trxFile : trxDir.listFiles())
//		for(int i = 0 ; i < 10; i++)
		{
			final File trxFileToRead = trxFile;
//			final File trxFileToRead = trxDir.listFiles()[i]; 
//			System.out.println(trxFileToRead.getAbsolutePath());
			
			Thread t = new Thread(new Runnable() {
				
				@Override
				public void run() {
					FileReader fileReader = null;
					try {
						fileReader = new FileReader(trxFileToRead);
					} catch (FileNotFoundException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					
					BufferedReader reader = new BufferedReader(fileReader);
					String line = null;
					try {
						while((line = reader.readLine()) != null )
						{
							
							try {
								trxsList.add(new TrxTuple(line.split(";")));
							} catch (ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
			});
			threads.add(t);
			t.start();
		}
		
		for(Thread t : threads)
		{
			try {
				t.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public  Map<Integer, AtomicInteger> queryData() {
		
		System.out.println(trxsList.size());
		final int batchSize = 500000;
		int n = trxsList.size() / batchSize;
		List<Thread> threads = new ArrayList<>();
		for(int i = 0 ; i < n; i++)
		{
			final Integer start = i * batchSize;
			Thread t = new Thread(new Runnable() {
				
				@Override
				public void run() {
					int len = trxsList.size() - start;
					if (len > batchSize) len = batchSize; 
					for(int i = 0; i < len; i++)
					{
						TrxTuple trx = trxsList.get(i+start);
						trxByAcidsCount.get(trx.acid).incrementAndGet();
					}
					
				}
			});
			threads.add(t);
			t.start();
		}
		for(Thread t : threads)
		{
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		
		return trxByAcidsCount;
		
	}
	

	
	 class TrxTuple{
//		byte[] merid;
		public int acid;
//		Date date;
		byte[]date;
		byte[] trxid;
		int amount;
		
		public TrxTuple(String[] props) throws ParseException
		{
			this(props[0],props[1],props[2],props[3],props[4]);
		}
		
		public TrxTuple(String acid, String merid,  String date, String trxid, String amount) throws ParseException {
			super();
			this.acid = Integer.parseInt(acid.split("-")[0]) * 1000 + Integer.parseInt(acid.split("-")[1]);
//			this.merid = merid.getBytes();
			this.trxid = trxid.getBytes();
			
//			this.date = sdf.parse(date);
			this.date = date.getBytes();
			this.amount = Integer.parseInt(amount);
			
			
			trxByAcidsCount.put(this.acid, new AtomicInteger(0));
		}
		
	}
}
