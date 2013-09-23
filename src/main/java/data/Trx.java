package data;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Trx {
	String acid;
	String merid;
	Date trxDate;
	String id;
	double amount;
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
	
	public Trx(String id, String acid, String merid, Date trxDate, double amount) 
	{
		this.id = id;
		this.acid = acid;
		this.merid = merid;	
		this.trxDate = trxDate;
		this.amount = amount;
	}

	public static Trx parse(String trxLine) throws ParseException
	{
//		System.out.println("Trx#init");
//		System.out.println(trxLine == null? "line = null": trxLine);
		String[] parts = trxLine.split(";");
		String acid = parts[0];
		String merid = parts[1];
		Date trxDate = sdf.parse(parts[2]);
		String id = parts[3];
		double amount = Double.parseDouble(parts[4]);
		return new Trx(id,acid,merid,trxDate,amount);
	}
	
	
	public String getAcid() {
		return acid;
	}
	public void setAcid(String acid) {
		this.acid = acid;
	}
	public String getMerid() {
		return merid;
	}
	public void setMerid(String merid) {
		this.merid = merid;
	}
	public Date getTrxDate() {
		return trxDate;
	}
	public void setTrxDate(Date trxDate) {
		this.trxDate = trxDate;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public double getAmount() {
		return amount;
	}
	public void setAmount(double amount) {
		this.amount = amount;
	}
	
	
	
	
}
