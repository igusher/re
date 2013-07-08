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
	void storeTrx(List<Trx> trxs);
	
	int getAcidsNum(REQuery reQuery);
}
