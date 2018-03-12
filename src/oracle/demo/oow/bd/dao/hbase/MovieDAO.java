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
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.to.CastCrewTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

public class MovieDAO {

	/**
	 * 添加movie-info的数据
	 * 
	 * @param movieTO
	 * @return
	 */
	public void insertMovie(MovieTO movieTO) {
		HBaseDB db = HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(), ConstantsHBase.FAMILY_MOVIE_MOVIE,
				ConstantsHBase.QUALIFIER_MOVIE_ORIGINAL_TITLE, movieTO.getTitle());
		db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(), ConstantsHBase.FAMILY_MOVIE_MOVIE,
				ConstantsHBase.QUALIFIER_MOVIE_OVERVIEW, movieTO.getOverview());
		db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(), ConstantsHBase.FAMILY_MOVIE_MOVIE,
				ConstantsHBase.QUALIFIER_MOVIE_POSTER_PATH, movieTO.getPosterPath());
		db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(), ConstantsHBase.FAMILY_MOVIE_MOVIE,
				ConstantsHBase.QUALIFIER_MOVIE_RELEASE_DATE, movieTO.getDate());
		db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(), ConstantsHBase.FAMILY_MOVIE_MOVIE,
				ConstantsHBase.QUALIFIER_MOVIE_VOTE_COUNT, movieTO.getVoteCount());
		db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(), ConstantsHBase.FAMILY_MOVIE_MOVIE,
				ConstantsHBase.QUALIFIER_MOVIE_RUNTIME, movieTO.getRunTime());
		db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(), ConstantsHBase.FAMILY_MOVIE_MOVIE,
				ConstantsHBase.QUALIFIER_MOVIE_POPULARITY, movieTO.getPopularity());
		try {
			insertMovieGenres(movieTO);
		} catch (Exception e) {
			e.printStackTrace();
		}
	} // insertMovie

	/**
	 * 写入电影--分类映射,以及添加分类数据
	 * 
	 * @param movieTO
	 * @throws Exception
	 */
	private void insertMovieGenres(MovieTO movieTO) throws Exception {
		HBaseDB db = HBaseDB.getInstance();
		ArrayList<GenreTO> genres = movieTO.getGenres();
		GenreDAO genreDAO = new GenreDAO();
		for (GenreTO genreTO : genres) {
			db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId() + "_" + genreTO.getId(),
					ConstantsHBase.FAMILY_MOVIE_GENRE, ConstantsHBase.QUALIFIER_MOVIE_GENRE_ID, genreTO.getId());
			db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId() + "_" + genreTO.getId(),
					ConstantsHBase.FAMILY_MOVIE_GENRE, ConstantsHBase.QUALIFIER_MOVIE_GENRE_NAME, genreTO.getName());
			if (!genreDAO.isExist(genreTO)) {
				genreDAO.insertGenre(genreTO);
			}
			genreDAO.insertGenre(genreTO);
			System.out.println("insert movieTO............");
			genreDAO.insertGenreMovie(movieTO, genreTO);

		}
	}

	/**
	 * movie--cast的映射
	 * 
	 * @param castTO
	 * @param movieId
	 */
	public void insertMovieCast(CastTO castTO, int movieId) {
		HBaseDB db = HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_MOVIE, movieId + "_" + castTO.getId(), ConstantsHBase.FAMILY_MOVIE_CAST,
				ConstantsHBase.QUALIFIER_MOVIE_CAST_ID, castTO.getId());
	}

	/**
	 * movie--crew的映射
	 * 
	 * @param crewTO
	 * @param valueOf
	 */
	public void insertMovieCrew(CrewTO crewTO, int movieId) {
		HBaseDB db = HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_MOVIE, movieId + "_" + crewTO.getId(), ConstantsHBase.FAMILY_MOVIE_CREW,
				ConstantsHBase.QUALIFIER_MOVIE_CREW_ID, crewTO.getId());
	}

	/**
	 * 获取电影详细信息,包括基本信息,genre,cast,crew******************************************
	 * @param movieId
	 * @return
	 */
	public MovieTO getMovieDetailById(int movieId) {
		
		MovieTO movieTO = getMovieById(movieId);
		
		// 设置genres
		ArrayList<GenreTO> genreTOs = getGenresByMovieId(movieId);
		System.out.println("genreTOs.size....." + genreTOs.size());
		movieTO.setGenres(genreTOs);
		
		CastCrewTO castCrewTO = new CastCrewTO();
		
		// 设置crews
		List<CrewTO> crewList = getCrewsByMovieId(movieId);
		System.out.println("crewList.size.........." + crewList.size());
		castCrewTO.setCrewList(crewList);
		
		// 设置casts
		List<CastTO> castList = getCastsByMovieId(movieId);
		System.out.println("castList.size....." + castList.size());
		castCrewTO.setCastList(castList);
		
		movieTO.setCastCrewTO(castCrewTO);

		return movieTO;
	}

	/**
	 * 根据movieid获取crews
	 * 
	 * @param movieId
	 * @return
	 */
	private List<CrewTO> getCrewsByMovieId(int movieId) {
		System.out.println("movieId.......crew..........." + movieId);
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_MOVIE);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CREW));
		Filter filter = new PrefixFilter(Bytes.toBytes(movieId + "_"));
		scan.setFilter(filter);

		ResultScanner resultScanner = null;

		try {
			resultScanner = table.getScanner(scan);
		} catch (Exception e) {
			e.printStackTrace();
		}

		List<CrewTO> crewTOs = new ArrayList<>();
		CrewTO crewTO = null;
		if (resultScanner != null) {
			Iterator<Result> iterator = resultScanner.iterator();
			CrewDAO crewDAO = new CrewDAO();
			while (iterator.hasNext()) {
				Result result = iterator.next();
				if (result != null && !result.isEmpty()) {
					int crewId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CREW),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_CREW_ID)));
					System.out.println("crewId............" + crewId);
					crewTO = crewDAO.getCrewById(crewId);
					crewTOs.add(crewTO);
				}
			}
		}
		return crewTOs;
	}

	/**
	 * 根据movieid获取casts
	 * 
	 * @param movieId
	 * @return
	 */
	private List<CastTO> getCastsByMovieId(int movieId) {
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_MOVIE);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CAST));
		Filter filter = new PrefixFilter(Bytes.toBytes(movieId + "_"));
		scan.setFilter(filter);

		ResultScanner resultScanner = null;

		try {
			resultScanner = table.getScanner(scan);
		} catch (Exception e) {
			e.printStackTrace();
		}
		List<CastTO> castTOs = new ArrayList<>();
		CastTO castTO = null;
		if (resultScanner != null) {
			Iterator<Result> iterator = resultScanner.iterator();
			CastDAO castDAO = new CastDAO();
			while (iterator.hasNext()) {
				Result result = iterator.next();
				if (result != null && !result.isEmpty()) {
					System.out.println("getCastsByMovieId.....if.......");
					int castId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CAST),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_CAST_ID)));
					System.out.println("castId............" + castId);
					castTO = castDAO.getCastById(castId);
					castTOs.add(castTO);
				}
			}
		}
		return castTOs;
	}

	/**
	 * 根据movieid获取genres
	 * 
	 * @param movieId
	 * @return
	 */
	private ArrayList<GenreTO> getGenresByMovieId(int movieId) {
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_MOVIE);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE));
		Filter filter = new PrefixFilter(Bytes.toBytes(movieId + "_"));
		scan.setFilter(filter);

		ResultScanner resultScanner = null;

		try {
			resultScanner = table.getScanner(scan);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		ArrayList<GenreTO> genreTOs = new ArrayList<>();
		GenreTO genreTO = null;
		if (resultScanner != null) {
			Iterator<Result> iterator = resultScanner.iterator();
			GenreDAO genreDAO = new GenreDAO();
			while (iterator.hasNext()) {
				Result result = iterator.next();
				if (result != null && !result.isEmpty()) {
					int genreId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_GENRE_ID)));
					System.out.println("genreId............" + genreId);
					genreTO = genreDAO.getGenreById(genreId);
					genreTOs.add(genreTO);
				}
			}
		}
		return genreTOs;
	}

	/**
	 * 根据movieId获取电影信息
	 * 
	 * @param movieId
	 * @return
	 */
	public MovieTO getMovieById(int movieId) {
		
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_MOVIE);
		Get get = new Get(Bytes.toBytes(movieId));
		MovieTO movieTO = new MovieTO();
		try {
			Result result = table.get(get);
			movieTO.setId(movieId);
			movieTO.setTitle(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_ORIGINAL_TITLE))));
			movieTO.setOverview(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_OVERVIEW))));
			movieTO.setPosterPath(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POSTER_PATH))));
			movieTO.setDate(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RELEASE_DATE))));
			movieTO.setVoteCount(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_VOTE_COUNT))));
			movieTO.setRunTime(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RUNTIME))));
			movieTO.setPopularity(Bytes.toDouble(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POPULARITY))));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return movieTO;
	}

	public List<MovieTO> getMoviesCurrentWatchList(int custId, int activityType) {
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
		Scan scan = new Scan();
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID), CompareOp.EQUAL, Bytes.toBytes(custId));
		Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY), CompareOp.EQUAL,
				Bytes.toBytes(activityType));
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
					int movieId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID)));
					movieTO = movieDAO.getMovieById(movieId);
					movieTOs.add(movieTO);
				}
			}
		}
		return movieTOs;
	}

	public List<MovieTO> getMoviesCurrentWatchList(int activityType) {
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
		Scan scan = new Scan();
		Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY), CompareOp.EQUAL,
				Bytes.toBytes(activityType));
		FilterList filterList = new FilterList(filter2);
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
					int movieId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID)));
					movieTO = movieDAO.getMovieById(movieId);
					movieTOs.add(movieTO);
				}
			}
		}
		return movieTOs;
	}
}
