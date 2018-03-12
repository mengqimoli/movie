package oracle.demo.oow.bd.dao.hbase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.to.MovieTO;

/**
 * This class is used to access recommended movie data for customer
 */
public class CustomerRatingDAO extends BaseDAO {

	private static Connection conn = null;

	public CustomerRatingDAO() {
		super();
		if (conn == null) {
			conn = getOraConnect(Constant.DB_DEMO_USER, Constant.DEMO_PASSWORD);
		}
	} // CustomerRatingDAO

	public void insertCustomerRating(int userId, int movieId, int rating) {
		String insert = null;
		PreparedStatement stmt = null;
		insert = "INSERT INTO cust_rating (userId, movieId, rating)  VALUES (?, ?, ?)";
		try {
			if (conn != null) {
				stmt = conn.prepareStatement(insert);
				stmt.setInt(1, userId);
				stmt.setInt(2, movieId);
				stmt.setInt(3, rating);
				stmt.execute();
				stmt.close();
				System.out.println("INFO: Customer: " + userId + " Movie: " + movieId + " rating: " + rating);
			}

		} catch (SQLException e) {
			System.out.println(e.getErrorCode() + ":" + e.getMessage());
		}
	}

	public void deleteCustomerRating(int userId) {
		String delete = null;
		PreparedStatement stmt = null;

		delete = "DELETE FROM cust_rating WHERE userId = ?";
		try {
			if (conn != null) {
				stmt = conn.prepareStatement(delete);
				stmt.setInt(1, userId);
				stmt.execute();
				stmt.close();
			}
		} catch (SQLException e) {
			System.out.println(e.getErrorCode() + ":" + e.getMessage());
		}
	}

	public List<MovieTO> getMoviesByMood(int userId) {
		List<MovieTO> movieList = null;
		String search = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		MovieTO movieTO = null;
		MovieDAO movieDAO = new MovieDAO();
		String title = null;
		Hashtable<String, String> movieHash = new Hashtable<String, String>();

		search = "select movieId from recommend where userId = '" + userId + "' "
				+ "And movieId Not In (Select movieId from cust_rating Where userId = '" + userId
				+ "') order by rating DESC";
		try {
			if (conn != null) {
				// initialize movieList only when connection is successful
				movieList = new ArrayList<MovieTO>();
				stmt = conn.prepareStatement(search);
				rs = stmt.executeQuery();
				while (rs.next()) {
					// Retrieve by column name
					int id = rs.getInt(1);
					System.out.println(id);
					// create new object
					movieTO = movieDAO.getMovieById(id);
					if (movieTO != null) {
						title = movieTO.getTitle();
						// Make sure movie title doesn't exist before in the
						// movieHash
						if (!movieHash.containsKey(title)) {
							movieHash.put(title, title);
							movieList.add(movieTO);
						}
					} // if (movieTO != null)
				} // EOF while
			} // EOF if (conn!=null)
		} catch (Exception e) {
			// No Database is running, so can not recommend item-item similarity
		}
		return movieList;
	}
}
