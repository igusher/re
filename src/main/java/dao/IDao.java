package dao;

import java.util.List;

import data.Acid;
import data.Merid;
import data.REQuery;
import data.Trx;

public interface IDao {
	void erase();
	
	void storeAcids(List<Acid> acids);
	void storeMerids(List<Merid> merids);
	int storeTrxs(List<Trx> trxs);
	void storeInsees(List<String> insees);
	
	boolean storeTrx(Trx trx);
	
	int getAcidsNum(REQuery reQuery);
}
