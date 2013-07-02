package data;

import java.text.DateFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class Acid {
	String id;
	Gender gender;
	Date birthDate;
	
	
	public Acid(String id, Gender gender, Date birthDate) {
		super();	
		setUp(id, gender, birthDate);
	}
	
	public Acid(String s) throws ParseException
	{
		String[] parts = s.split(";");
		setUp(parts[0],parts[1],parts[2]);
	}
	
	public void setUp(String id, String gender, String birthDate) throws ParseException
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-DD", Locale.ENGLISH);
		Date pBD = sdf.parse(birthDate);

		Gender pGender;
		switch(gender){
			case "M": pGender = Gender.MALE; break; 
			case "F": pGender = Gender.FEMALE; break;
			default: throw new ParseException("genderParseError", 0);
		}
		
		setUp(id, pGender, pBD);
	}
	
	
	
	public void setUp(String id, Gender gender, Date birthDate)
	{
		this.id = id;
		this.gender = gender;
		this.birthDate = birthDate;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public Gender getGender() {
		return gender;
	}
	public void setGender(Gender gender) {
		this.gender = gender;
	}
	public Date getBirthDate() {
		return birthDate;
	}
	public void setBirthDate(Date birthDate) {
		this.birthDate = birthDate;
	}
	
	
}
