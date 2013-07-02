package data;

import java.util.Date;
import java.util.List;

public class REQuery {
	Gender gender;
	int minAge;
	int maxAge;
	List<String> insees;
	int minTrxNum;
	int maxTrxNum;
	Date fromDate;
	Date toDate;
	String merid;
	CustomerProfile custmerProfile;
}


enum CustomerType{
	NEW, EXISTING
}

enum CustomerProfile{
	SILVER, GOLD, PLATINUM, ALL
}