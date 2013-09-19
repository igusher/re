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
	
	void storeAcid(Acid acid);
	void storeMerid(Merid merid);
	boolean storeTrx(Trx trx);
	void storeInsee(String insee);
	
	int getAcidsNum(REQuery reQuery);
}
