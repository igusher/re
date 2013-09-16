package data;

import java.util.Date;
import java.util.List;

public class REQuery {
	Gender gender;
	AgeGroup ageGroup;
	List<String> insees;
	int minTrxNum;
	int maxTrxNum;
	Date fromDate;
	Date toDate;
	String merid;
	CustomerProfile custmerProfile;
	
		public REQuery(Gender gender, AgeGroup ageGroup, List<String> insees,
			int minTrxNum, int maxTrxNum, Date fromDate, Date toDate,
			String merid, CustomerProfile custmerProfile) {
		super();
		this.gender = gender;
		this.ageGroup = ageGroup;
		this.insees = insees;
		this.minTrxNum = minTrxNum;
		this.maxTrxNum = maxTrxNum;
		this.fromDate = fromDate;
		this.toDate = toDate;
		this.merid = merid;
		this.custmerProfile = custmerProfile;
	}

	public char getGenderChar()
	{
		return gender.toString().charAt(0);
	}
	
	public Gender getGender() {
		return gender;
	}
	public void setGender(Gender gender) {
		this.gender = gender;
	}

	public AgeGroup getAgeGroup() {
		return ageGroup;
	}

	public void setAgeGroup(AgeGroup ageGroup) {
		this.ageGroup = ageGroup;
	}

	public List<String> getInsees() {
		return insees;
	}
	public void setInsees(List<String> insees) {
		this.insees = insees;
	}
	public int getMinTrxNum() {
		return minTrxNum;
	}
	public void setMinTrxNum(int minTrxNum) {
		this.minTrxNum = minTrxNum;
	}
	public int getMaxTrxNum() {
		return maxTrxNum;
	}
	public void setMaxTrxNum(int maxTrxNum) {
		this.maxTrxNum = maxTrxNum;
	}
	public Date getFromDate() {
		return fromDate;
	}
	public void setFromDate(Date fromDate) {
		this.fromDate = fromDate;
	}
	public Date getToDate() {
		return toDate;
	}
	public void setToDate(Date toDate) {
		this.toDate = toDate;
	}
	public String getMerid() {
		return merid;
	}
	public void setMerid(String merid) {
		this.merid = merid;
	}
	public CustomerProfile getCustmerProfile() {
		return custmerProfile;
	}
	public void setCustmerProfile(CustomerProfile custmerProfile) {
		this.custmerProfile = custmerProfile;
	}
	
	
}


enum CustomerType{
	NEW, EXISTING
}

enum CustomerProfile{
	SILVER, GOLD, PLATINUM, ALL
}