package data;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Trx {
	String acid;
	String merid;
	Date trxDate;
	String id;
	double amount;
	
	public Trx(String line) throws ParseException
	{
		String[] parts = line.split(";");
		acid = parts[0];
		merid = parts[1];
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-DD");
		trxDate = sdf.parse(parts[2]);
		id = parts[3];
		amount = Double.parseDouble(parts[4]);
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
