package oracle.demo.oow.bd.util.hbase.loder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.dao.hbase.CastDAO;
import oracle.demo.oow.bd.dao.hbase.CrewDAO;
import oracle.demo.oow.bd.dao.hbase.MovieDAO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.MovieTO;

/**
 * 导入用户相关数据数据 在movie-info.out中
 * 
 * @author Luff
 *
 */
public class MovieLoder {

	public static void main(String[] args) {
		MovieLoder movieLoder = new MovieLoder();
		try {
			movieLoder.uploadMovieCrew();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void uploadMovieInfo() throws IOException {
		FileReader fr = null;
		try {
			fr = new FileReader(Constant.WIKI_MOVIE_INFO_FILE_NAME);
			BufferedReader br = new BufferedReader(fr);
			String jsonTxt = null;
			MovieTO movieTO = null;
			MovieDAO movieDAO = new MovieDAO();
			int count = 1;

			// Each line in the file is the JSON string

			// Construct MovieTO from JSON object
			while ((jsonTxt = br.readLine()) != null) {

				try {
					movieTO = new MovieTO(jsonTxt.trim());
				} catch (Exception e) {
					System.out.println("ERROR: Not able to parse the json string: \t" + jsonTxt);
				}
				if (movieTO != null && !movieTO.isAdult()) {
					movieDAO.insertMovie(movieTO);
				} // EOF if
			} // EOF while

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			fr.close();
		}
	} // uploadMovies

	public void uploadMovieCast() throws IOException {
		FileReader fr = null;
		try {
			fr = new FileReader(Constant.WIKI_MOVIE_CAST_FILE_NAME);
			BufferedReader br = new BufferedReader(fr);
			String jsonTxt = null;
			CastTO castTO = null;
			CastDAO castDAO = new CastDAO();
			int count = 1;
			// Each line in the file is the JSON string
			// Construct MovieTO from JSON object
			while ((jsonTxt = br.readLine()) != null) {
				try {
					castTO = new CastTO(jsonTxt.trim());
				} catch (Exception e) {
					System.out.println("ERROR: Not able to parse the json string: \t" + jsonTxt);
				}
				if (castTO != null) {
					castDAO.insertCastInfo(castTO);
				} // EOF if
			} // EOF while
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			fr.close();
		}

	} // uploadMovies

	/**
	 * This method reads the file with movie Crew records and load it into
	 * kv-store one movie at a time
	 * 
	 * @throws IOException
	 */
	public void uploadMovieCrew() throws IOException {
		FileReader fr = null;
		try {
			fr = new FileReader(Constant.WIKI_MOVIE_CREW_FILE_NAME);
			BufferedReader br = new BufferedReader(fr);
			String jsonTxt = null;
			CrewTO crewTO = null;
			int count = 1;

			// Each line in the file is the JSON string

			// Construct MovieTO from JSON object
			while ((jsonTxt = br.readLine()) != null) {
				try {
					crewTO = new CrewTO(jsonTxt.trim());
				} catch (Exception e) {
					System.out.println("ERROR: Not able to parse the json string: \t" + jsonTxt);
				}

				if (crewTO != null) {
					CrewDAO crewDAO = new CrewDAO();

					crewDAO.insertCrewInfo(crewTO);

				} // EOF if

			} // EOF while
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			fr.close();
		}
	}
}
