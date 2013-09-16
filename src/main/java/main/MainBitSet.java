package main;

import java.io.IOException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Scanner;

import logic.ILogic;
import logic.Logic;

import data.AgeGroup;
import data.Gender;
import data.REQuery;

public class MainBitSet {
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	static ILogic logic = new Logic();

	/**
	 * @param args
	 * @throws IOException
	 * @throws ParseException
	 */
	public static void main(String[] args) throws ParseException, IOException {
		logic.setUp();
		
		while(true)
		{
			System.out.println("Waiting For Requests");
			REQuery reQuery = readNextQuery();
			if(reQuery == null)
				return;
			int responseAcidNum = logic.getAcidsNum(reQuery);
			System.out.println(responseAcidNum);
		}
	}

	private static REQuery readNextQuery() {
		
		Scanner scanner = new Scanner(System.in);
		while (true) {
			try {
				String merid = scanner.nextLine(); // merid
				if (merid.contains("exit")) {
					return null;
				}
				String listOfInsees = scanner.nextLine(); // list of insee. delimiter is blank space (' ')
				String genderString = scanner.nextLine(); // gender. Either symbol 'M' or 'F'
				int ageInt = Integer.parseInt(scanner.nextLine()); // age group values from 1 till 4 including

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
				Gender gender = null;
				if (genderString.equals("M"))
					gender = Gender.MALE;
				else
					gender = Gender.FEMALE;
				String[] insees = listOfInsees.split(" ");

				return new REQuery(gender,ageGr, Arrays.asList(insees),fromTrxsNum,toTrxsNum, fromDate,toDate,merid,null );
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Input data parse error. Try again... (Type 'exit' to stop)");
			}

		}
	}

}
