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
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.GenreMovieTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

public class GenreDAO {

	/**
	 * 插入genre信息
	 * 
	 * @param genreDAO
	 */
	public void insertGenre(GenreTO genreTO) {
		HBaseDB db = HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_GENRE, genreTO.getId(), ConstantsHBase.FAMILY_GENRE_GENRE,
				ConstantsHBase.QUALIFIER_GENRE_NAME, genreTO.getName());
	}

	/**
	 * genreTo是否存在,存在返回true
	 * 
	 * @param genreTO
	 * @return
	 */
	public boolean isExist(GenreTO genreTO) {
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_GENRE);
		Get get = new Get(Bytes.toBytes(genreTO.getId()));

		try {
			Result result = table.get(get);
			if (result != null) {
				return false;
			}
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public void insertGenreMovie(MovieTO movieTO, GenreTO genreTO) {
		HBaseDB db = HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_GENRE, genreTO.getId() + "_" + movieTO.getId(), ConstantsHBase.FAMILY_GENRE_MOVIE,
				ConstantsHBase.QUALIFIER_GENRE_MOVIE_ID, movieTO.getId());
	}

	public List<GenreMovieTO> getMovies4Customer(int custId, int movieMaxCount, int genreMaxCount) {
		List<GenreMovieTO> genreMovieTOs = new ArrayList<>();
		Scan scan = new Scan();
		Filter filter = new PageFilter(genreMaxCount);
		scan.setFilter(filter);
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE));
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_GENRE);
		// 全局扫描
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			Iterator<Result> iterator = resultScanner.iterator();
			GenreTO genreTO = null;
			while (iterator.hasNext()) {
				genreTO = new GenreTO();
				Result result = iterator.next();
				genreTO.setId(Bytes.toInt(result.getRow()));
				genreTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_NAME))));
				GenreMovieTO genreMovieTO = new GenreMovieTO();
				genreMovieTO.setGenreTO(genreTO);
				genreMovieTOs.add(genreMovieTO);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return genreMovieTOs;
	}

	/**
	 * 根据id获取genre
	 * @param genreId
	 * @return
	 */
	public GenreTO getGenreById(int genreId) {
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_GENRE);
		Get get = new Get(Bytes.toBytes(genreId));
		GenreTO genreTO = new GenreTO();
		try {
			Result result = table.get(get);
			genreTO.setId(genreId);
			genreTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_NAME))));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return genreTO;
	}
}
