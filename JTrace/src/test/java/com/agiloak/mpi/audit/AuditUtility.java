package com.agiloak.mpi.audit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.agiloak.mpi.MpiException;

public class AuditUtility {

	public static void makeUpdatedByMandatory(Connection conn, boolean mandatory) throws MpiException {

		String updateSql;
		if (mandatory) {
			updateSql = "ALTER TABLE audit ALTER COLUMN updatedby SET NOT NULL;";
		} else {
			updateSql = "ALTER TABLE audit ALTER COLUMN updatedby DROP NOT NULL;";
		}
		
		PreparedStatement preparedStatement1 = null;
		PreparedStatement preparedStatement2 = null;
		
		try {

			preparedStatement1 = conn.prepareStatement("UPDATE audit set updatedby=''; ");
			preparedStatement2 = conn.prepareStatement(updateSql);
			preparedStatement1.executeUpdate();
			preparedStatement2.executeUpdate();
					 
		} catch (SQLException e) {
			System.out.print(e);
			throw new MpiException("Audit alter failed");

		} finally {

			if (preparedStatement1 != null) {
				try {
					preparedStatement1.close();
				} catch (SQLException e) {
					System.out.print(e);
					throw new MpiException("Audit alter failed");
				}
			}
			if (preparedStatement2 != null) {
				try {
					preparedStatement2.close();
				} catch (SQLException e) {
					System.out.print(e);
					throw new MpiException("Audit alter failed");
				}
			}

		}
	}
}
