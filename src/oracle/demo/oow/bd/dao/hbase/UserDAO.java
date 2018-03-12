package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.CustomerTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.StringUtil;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

public class UserDAO {
	public int MOVIE_MAX_COUNT = 25;

	public ActivityTO getMovieRating(int custId, int movieId) {
		ActivityTO activityTO = null;
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
		Scan scan = new Scan();
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID), CompareFilter.CompareOp.EQUAL,
				new SubstringComparator(String.valueOf(custId)));
		((SingleColumnValueFilter) filter).setFilterIfMissing(true);
		Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID), CompareFilter.CompareOp.EQUAL,
				new SubstringComparator(String.valueOf(movieId)));
		((SingleColumnValueFilter) filter2).setFilterIfMissing(true);
		FilterList filterList = new FilterList(filter, filter2);
		scan.setFilter(filterList);
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			Result result = resultScanner.next();
			if (result != null) {
				activityTO.setCustId(custId);
				activityTO.setMovieId(movieId);
			}
			resultScanner.close();
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("sdfds.........."+activityTO);
		return activityTO;
	}

	/**
	 * 根据分类id获取电影信息
	 * 
	 * @param custId
	 * @param genreId
	 * @return
	 */
	public List<MovieTO> getMovies4CustomerByGenre(int custId, int genreId) {
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_GENRE);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_MOVIE));
		Filter filter = new PrefixFilter(Bytes.toBytes(genreId + "_"));
		Filter filter2 = new PageFilter(MOVIE_MAX_COUNT);
		FilterList filterList = new FilterList(filter, filter2);
		scan.setFilter(filterList);

		ResultScanner resultScanner = null;

		try {
			resultScanner = table.getScanner(scan);
		} catch (Exception e) {
			e.printStackTrace();
		}

		List<MovieTO> movieTOs = new ArrayList<>();
		MovieTO movieTO = null;
		if (resultScanner != null) {
			Iterator<Result> iterator = resultScanner.iterator();
			MovieDAO movieDAO = new MovieDAO();
			while (iterator.hasNext()) {
				Result result = iterator.next();
				if (result != null && !result.isEmpty()) {
					int movieId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_MOVIE_ID)));
					movieTO = movieDAO.getMovieById(movieId);

					if (StringUtil.isNotEmpty(movieTO.getPosterPath())) {
						movieTO.setOrder(100);
					} else {
						movieTO.setOrder(0);
					}
					movieTOs.add(movieTO);
				}
			}
		}
		return movieTOs;
	}

	// 添加user中的info和id列族
	public void insert(CustomerTO custTO) {
		HBaseDB baseDB = HBaseDB.getInstance();
		Table table = baseDB.getTable("user");
		if (table != null) {
			Put put = new Put(Bytes.toBytes(custTO.getUserName()));
			// username->id
			put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("id"), Bytes.toBytes(custTO.getId()));
			// 用户基本数据
			Put put2 = new Put(Bytes.toBytes(custTO.getId()));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(custTO.getName()));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("email"), Bytes.toBytes(custTO.getEmail()));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("username"), Bytes.toBytes(custTO.getUserName()));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"), Bytes.toBytes(custTO.getPassword()));

			List<Put> puts = new ArrayList();
			puts.add(put);
			puts.add(put2);
			try {
				table.put(puts);
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public CustomerTO getCustomerByCredential(String username, String password) {
		System.out.println("UserDAO..............");
		CustomerTO customerTO = null;
		// 首先通过username查询id
		int id = getIdByUsername(username);
		// 通过id查找基本信息
		if (id > 0) {
			customerTO = getInfoById(id);
			if (customerTO != null) {
				if (!password.equals(customerTO.getPassword())) {
					customerTO = null;
				}
			}
		}
		return customerTO;
	}

	private CustomerTO getInfoById(int id) {
		HBaseDB baseDB = HBaseDB.getInstance();
		Table table = baseDB.getTable("user");
		Get get = new Get(Bytes.toBytes(id));
		CustomerTO customerTO = new CustomerTO();
		try {
			Result result = table.get(get);
			customerTO.setId(id);
			customerTO.setEmail(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("email"))));
			customerTO.setName(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
			customerTO.setUserName(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("username"))));
			customerTO.setPassword(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password"))));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return customerTO;
	}

	public int getIdByUsername(String username) {
		HBaseDB baseDB = HBaseDB.getInstance();
		Table table = baseDB.getTable("user");
		Get get = new Get(Bytes.toBytes(username));
		int id = 0;
		try {
			Result result = table.get(get);
			id = Bytes.toInt(result.getValue(Bytes.toBytes("id"), Bytes.toBytes("id")));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return id;
	}

}
