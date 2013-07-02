package dao;

import java.util.List;

import data.Acid;
import data.Merid;
import data.Trx;

public interface IDao {
	void storeAcids(List<Acid> acids);
	void storeMerids(List<Merid> merids);
	void storeTrx(List<Trx> trxs);
}
