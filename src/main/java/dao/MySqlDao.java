package dao;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;



import data.Acid;
import data.Merid;
import data.REQuery;
import data.Trx;

public class MySqlDao implements IDao {

	Connection conn;
	
	public MySqlDao() {
		try{
			DriverManager.registerDriver(new com.mysql.jdbc.Driver());
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/RE", "root","secret");
		}
		catch(Exception exc)
		{
			exc.printStackTrace();
			throw new RuntimeException(exc);
		}
	}
	
	@Override
	public void storeAcids(List<Acid> acids)  {
		try{
			for(int i = 0; i < acids.size(); i=i+1000)
			{
				
				StringBuilder queryBuilder = new StringBuilder();
				queryBuilder.append("INSERT INTO acids (id, gender, birthdate) VALUES ");
				for(int j = 0 ; j < 1000; j++)
				{
					if (j != 0)
					{
						queryBuilder.append(",");
					}
					queryBuilder.append("(?,?,?)");
				}
				
				PreparedStatement prepStmt = conn.prepareStatement(queryBuilder.toString());
				
				for(int j = 0 ; j < 1000 && j < acids.size(); j++)
				{
					Acid acid = acids.get(j+i);
					prepStmt.setString(j*3+1, acid.getId());
					prepStmt.setInt(j*3+2, acid.getGender().ordinal());
					java.sql.Date sqlDate = new Date(acid.getBirthDate().getTime());
					prepStmt.setDate(j*3+3, sqlDate);
				}
				prepStmt.execute();
				
			}
		}
		catch(Exception exc)
		{
			exc.printStackTrace();
			throw new RuntimeException(exc);
		}
	}

	@Override
	public void storeMerids(List<Merid> merids) {
		try{
			StringBuilder queryBuilder = new StringBuilder();
			queryBuilder.append("INSERT INTO merids (id, insee, other) VALUES ");
			
			for(int j = 0 ; j < merids.size(); j++)
			{
				if (j != 0)
				{
					queryBuilder.append(",");
				}
				queryBuilder.append("(?,?,?)");
			}
			
			PreparedStatement prepStmt = conn.prepareStatement(queryBuilder.toString());
			
			for(int j = 0 ; j < merids.size(); j++)
			{
				Merid merid = merids.get(j);
				prepStmt.setString(j*3+1, merid.getId());
				prepStmt.setString(j*3+2, merid.getInsee());
				prepStmt.setString(j*3+3, merid.getOther());
			}
			prepStmt.execute();
		}
		catch(Exception exc)
		{
			exc.printStackTrace();
			throw new RuntimeException(exc);
		}
	}

	@Override
	public void storeTrx(List<Trx> trxs) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getAcidsNum(REQuery reQuery) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
