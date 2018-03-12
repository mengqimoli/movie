package oracle.demo.oow.bd.dao.hbase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.fastjson.JSONObject;

import oracle.demo.oow.bd.to.CastMovieTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

public class CastDAO {

	/**
	 * 根据cast获取movies
	 * @param castId
	 * @return
	 */
	public List<MovieTO> getMoviesByCast(int castId) {
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_CAST);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE));
		Filter filter = new PrefixFilter(Bytes.toBytes(castId + "_"));
		scan.setFilter(filter);
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
					System.out.println("getMoviesByCast........if........");
					int movieId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_MOVIE_ID)));
					System.out.println("movieId............" + movieId);
					movieTO = movieDAO.getMovieById(movieId);
					movieTOs.add(movieTO);
				}
			}
		}
		return movieTOs;
	}

	/**
	 * 导入cast数据
	 * 
	 * @param castTO
	 */
	public void insertCastInfo(CastTO castTO) {
		HBaseDB db = HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_CAST, castTO.getId(), ConstantsHBase.FAMILY_CAST_CAST,
				ConstantsHBase.QUALIFIER_CAST_NAME, castTO.getName());
		insertCastMovie(castTO);
	} // insertCastInfo

	/**
	 * 写入cast和movie映射的数据
	 * 
	 * @param castTO
	 */
	private void insertCastMovie(CastTO castTO) {
		HBaseDB db = HBaseDB.getInstance();
		List<CastMovieTO> castMovieList = castTO.getCastMovieList();
		MovieDAO movieDAO = new MovieDAO();
		for (CastMovieTO castMovieTO : castMovieList) {
			db.put(ConstantsHBase.TABLE_CAST, castTO.getId() + "_" + castMovieTO.getId(),
					ConstantsHBase.FAMILY_CAST_MOVIE, ConstantsHBase.QUALIFIER_CAST_MOVIE_ID, castMovieTO.getId());
			db.put(ConstantsHBase.TABLE_CAST, castTO.getId() + "_" + castMovieTO.getId(),
					ConstantsHBase.FAMILY_CAST_MOVIE, ConstantsHBase.QUALIFIER_CAST_CHARACTER,
					castMovieTO.getCharacter());
			db.put(ConstantsHBase.TABLE_CAST, castTO.getId() + "_" + castMovieTO.getId(),
					ConstantsHBase.FAMILY_CAST_MOVIE, ConstantsHBase.QUALIFIER_CAST_ORDER, castMovieTO.getOrder());
			movieDAO.insertMovieCast(castTO, castMovieTO.getId());
		}
		System.out.println("inset castTO............");
	}

	/**
	 * 通过id获取Cast
	 * 
	 * @param castId
	 * @return
	 */
	public CastTO getCastById(int castId) {
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_CAST);
		Get get = new Get(Bytes.toBytes(castId));
		CastTO castTO = null;
		try {
			Result result = table.get(get);

			// JSONObject jsonObject = new JSONObject();
			// jsonObject.put("id", castId);
			// jsonObject.put("name", new
			// String(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_CAST),
			// Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_NAME))));

			// castTO = new CastTO(jsonObject.toJSONString());
			castTO = new CastTO();
			castTO.setId(castId);
			castTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_CAST),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_NAME))));

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("castTO......." + castTO.toString());
		return castTO;
	}
}
