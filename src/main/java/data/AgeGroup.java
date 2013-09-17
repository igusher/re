package data;

public enum AgeGroup {
	G10_25, G25_40, G40_55, G55_70;
	
	public static AgeGroup parse(int ageGroup) {
		AgeGroup result = null;
		switch(ageGroup)
		{
			case 0: result = AgeGroup.G10_25; break;
			case 1: result = AgeGroup.G10_25; break;
			case 2: result = AgeGroup.G10_25; break;
			case 3: result = AgeGroup.G10_25; break;
			default: throw new IllegalArgumentException("Age Group can have values 1 - 4");
		}
		return result;
	}
}
