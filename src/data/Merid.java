package data;

public class Merid {
	String id;
	String insee;
	String other;
	
	public Merid(String line)
	{
		String[] parts = line.split(";");
		id = parts[0];
		insee = parts[1];
		other = parts[2];
	}

	public Merid(String id, String insee, String other) {
		super();
		this.id = id;
		this.insee = insee;
		this.other = other;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getInsee() {
		return insee;
	}
	public void setInsee(String insee) {
		this.insee = insee;
	}
	public String getOther() {
		return other;
	}
	public void setOther(String other) {
		this.other = other;
	}
	
	
	
	
}
