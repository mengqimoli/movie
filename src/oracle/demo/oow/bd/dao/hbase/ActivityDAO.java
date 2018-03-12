package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.pojo.ActivityType;
import oracle.demo.oow.bd.pojo.BooleanType;
import oracle.demo.oow.bd.pojo.RatingType;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.FileWriterUtil;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

public class ActivityDAO {

	MovieDAO movieDAO = null;

	private static Table activityTable = null;
	public final static String TABLE_NAME = "ACTIVITY";

	public ActivityDAO() {

	}

	/**
	 * 当前观看的电影
	 * 
	 * @param custId
	 * @return
	 */
	public List<MovieTO> getCustomerCurrentWatchList(int custId) {
		int activity = 5;
		MovieDAO movieDAO1 = new MovieDAO();
		List<MovieTO> movieList = movieDAO1.getMoviesCurrentWatchList(custId, activity);
		return movieList;
	}

	/**
	 * 
	 * @param custId
	 * @return
	 */
	public List<MovieTO> getCustomerBrowseList(int custId) {
		int activity = 5;
		MovieDAO movieDAO1 = new MovieDAO();
		List<MovieTO> movieList = movieDAO1.getMoviesCurrentWatchList(custId, activity);
		return movieList;
	}

	/**
	 * 已经观看的电影
	 * 
	 * @param custId
	 * @return
	 */
	public List<MovieTO> getCustomerHistoricWatchList(int custId) {
		int activity = 5;
		MovieDAO movieDAO1 = new MovieDAO();
		List<MovieTO> movieList = movieDAO1.getMoviesCurrentWatchList(custId, activity);
		return movieList;
	}

	/**
	 * 公共观看
	 * 
	 * @return
	 */
	public List<MovieTO> getCommonPlayList() {
		int activity = 5;
		MovieDAO movieDAO1 = new MovieDAO();
		List<MovieTO> movieList = movieDAO1.getMoviesCurrentWatchList(activity);
		System.out.println("movieList........."+movieList.size());
		return movieList;
	}

	public ActivityTO getActivityTO(int custId, int movieId) {
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
		Scan scan = new Scan();
		// 设置过滤器
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID), CompareOp.EQUAL, Bytes.toBytes(movieId));
		Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID), CompareOp.EQUAL, Bytes.toBytes(custId));
		FilterList filterList = new FilterList(filter, filter2);
		scan.setFilter(filterList);
		ResultScanner resultScanner = null;
		ActivityTO activityTO = new ActivityTO();
		try {
			resultScanner = table.getScanner(scan);
			if (resultScanner != null) {
				Iterator<Result> iterator = resultScanner.iterator();
				while (iterator.hasNext()) {
					Result result = iterator.next();
					if (result != null && !result.isEmpty()) {
						activityTO.setCustId(custId);
						activityTO.setMovieId(movieId);
						activityTO.setGenreId(
								Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
										Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID))));
						activityTO.setActivity(ActivityType.getType(
								Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
										Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY)))));
						activityTO.setRecommended(BooleanType.getType(
								Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
										Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RECOMMENDED)))));
						activityTO.setTimeStamp(
								Bytes.toLong(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
										Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TIME))));
						activityTO.setRating(RatingType.getType(
								Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
										Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING)))));
						activityTO.setPrice(
								Bytes.toDouble(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
										Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_PRICE))));
						activityTO.setPosition(
								Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
										Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_POSITION))));
					} else {
						activityTO = null;
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return activityTO;
	}

	public void insertCustomerActivity(ActivityTO activityTO) {
		int custId = 0;
		int movieId = 0;
		ActivityType activityType = null;
		String jsonTxt = null;

		if (activityTO != null) {
			
			deleteActivity(activityTO);
			
			jsonTxt = activityTO.getJsonTxt();
			System.out.println("User Activity| " + jsonTxt);

			/**
			 * This system out should write the content to the application log
			 * file.
			 */
			FileWriterUtil.writeOnFile(activityTO.getActivityJsonOriginal().toString());

			custId = activityTO.getCustId();
			movieId = activityTO.getMovieId();

			activityType = activityTO.getActivity();

			HBaseDB db = HBaseDB.getInstance();
			Long id = db.getId(ConstantsHBase.TABLE_GID, ConstantsHBase.FAMILY_GID_GID,
					ConstantsHBase.QUALIFIER_GID_ACTIVITY_ID);
			Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
			Put put = new Put(Bytes.toBytes(id));
			put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID), Bytes.toBytes(activityTO.getMovieId()));
			put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY),
					Bytes.toBytes(activityTO.getActivity().getValue()));
			put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID), Bytes.toBytes(activityTO.getGenreId()));
			put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_POSITION), Bytes.toBytes(activityTO.getPosition()));
			put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_PRICE), Bytes.toBytes(activityTO.getPrice()));
			put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING),
					Bytes.toBytes(activityTO.getRating().getValue()));
			put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RECOMMENDED),
					Bytes.toBytes(activityTO.isRecommended().getValue()));
			put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TIME), Bytes.toBytes(activityTO.getTimeStamp()));
			put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID), Bytes.toBytes(activityTO.getCustId()));

			try {
				table.put(put);
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		} // if (activityTO != null)

	} // insetCustomerActivity

	private void deleteActivity(ActivityTO activityTO) {
		int userId = activityTO.getCustId();
		int movieId = activityTO.getMovieId();
		ActivityTO activityTO2 = getActivityTO(userId,movieId);
		if(activityTO2 != null){
			HBaseDB db = HBaseDB.getInstance();
			Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
			Scan scan = new Scan();
			// 设置过滤器
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID), CompareOp.EQUAL, Bytes.toBytes(movieId));
			Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID), CompareOp.EQUAL, Bytes.toBytes(userId));
			FilterList filterList = new FilterList(filter, filter2);
			scan.setFilter(filterList);
			ResultScanner resultScanner = null;
			try {
				resultScanner = table.getScanner(scan);
				if (resultScanner != null) {
					Iterator<Result> iterator = resultScanner.iterator();
					while (iterator.hasNext()) {
						Result result = iterator.next();
						if (result != null && !result.isEmpty()) {
							Delete delete = new Delete(result.getRow());
							//delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"));
							table.delete(delete);
							System.out.println("删除成功");
						} 
					}
				}
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}// ActivityDAO
