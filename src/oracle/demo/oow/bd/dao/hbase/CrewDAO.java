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

import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

public class CrewDAO {

	/**
	 * 根据crew获取movies
	 * 
	 * @param castId
	 * @return
	 */
	public List<MovieTO> getMoviesByCrew(int crewId) {
		System.out.println("crewId..........." + crewId);
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_CREW);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_MOVIE));
		Filter filter = new PrefixFilter(Bytes.toBytes(crewId + "_"));
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
					System.out.println("getMoviesByCrew........if........");
					String movieId = Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_MOVIE),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_MOVIE_ID)));
					System.out.println("movieId............" + movieId);
					movieTO = movieDAO.getMovieById(Integer.valueOf(movieId));
					movieTOs.add(movieTO);
				}
			}
		}
		return movieTOs;
	}

	/**
	 * 添加crew数据
	 * 
	 * @param crewTO
	 */
	public void insertCrewInfo(CrewTO crewTO) {
		HBaseDB db = HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_CREW, crewTO.getId(), ConstantsHBase.FAMILY_CREW_CREW,
				ConstantsHBase.QUALIFIER_CREW_NAME, crewTO.getName());
		db.put(ConstantsHBase.TABLE_CREW, crewTO.getId(), ConstantsHBase.FAMILY_CREW_CREW,
				ConstantsHBase.QUALIFIER_CREW_JOB, crewTO.getJob());

		insertCrewMovie(crewTO);
	}

	/**
	 * 导入crew--movie的数据
	 * 
	 * @param crewTO
	 */
	private void insertCrewMovie(CrewTO crewTO) {
		HBaseDB db = HBaseDB.getInstance();
		List<String> movieList = crewTO.getMovieList();
		MovieDAO movieDAO = new MovieDAO();
		for (String movieId : movieList) {
			db.put(ConstantsHBase.TABLE_CREW, crewTO.getId() + "_" + movieId, ConstantsHBase.FAMILY_CREW_MOVIE,
					ConstantsHBase.QUALIFIER_CREW_MOVIE_ID, movieId);
			movieDAO.insertMovieCrew(crewTO, Integer.valueOf(movieId));
			System.out.println("insert crewTO.................." + movieId);
		}
	}

	/**
	 * 通过Id获取Crew
	 * 
	 * @param crewId
	 * @return
	 */
	public CrewTO getCrewById(int crewId) {
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_CREW);
		Get get = new Get(Bytes.toBytes(crewId));
		CrewTO crewTO = null;
		try {
			Result result = table.get(get);

			JSONObject jsonObject = new JSONObject();
			jsonObject.put("id", crewId);
			jsonObject.put("name", new String(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_NAME))));
			jsonObject.put("job", new String(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_JOB))));

			crewTO = new CrewTO(jsonObject.toJSONString());

			// crewId.setId(crewId);
			// crewTO.setJob(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW),
			// Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_JOB))));
			// crewTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW),
			// Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_NAME))));

		} catch (Exception e) {
			e.printStackTrace();
		}
		return crewTO;
	}
}
