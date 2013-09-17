package data;

public enum Gender{
	MALE, FEMALE, ALL;
	
	public static Gender parse(String gender)
	{
		if(gender.equals("M"))
			return Gender.MALE;
		if(gender.equals("F"))
			return Gender.FEMALE;
		if(gender.equals("ALL"))
			return Gender.ALL;
		throw new IllegalArgumentException("gender may take only values: [M,F,ALL]. Actual value is " + gender);
	}
}

