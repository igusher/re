package logic;

import java.io.IOException;
import java.text.ParseException;

import data.REQuery;

public interface ILogic {
	void setUp() throws ParseException, IOException;
	int getAcidsNum(REQuery reQuery);
}
