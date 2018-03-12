package oracle.demo.oow.bd.ui;

import java.io.IOException;
import java.util.Date;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import oracle.demo.oow.bd.dao.hbase.ActivityDAO;
import oracle.demo.oow.bd.dao.hbase.CustomerRatingDAO;
import oracle.demo.oow.bd.dao.hbase.UserDAO;
import oracle.demo.oow.bd.pojo.ActivityType;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.CustomerTO;

public class logIn extends HttpServlet {
	// private static final String CONTENT_TYPE = "text/html;
	// charset=windows-1252";
	private String loginPage = "login.jsp";
	private String indexPage = "index.jsp";

	public void init(ServletConfig config) throws ServletException {
		super.init(config);
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		 CustomerRatingDAO custRatingDAO = new CustomerRatingDAO();

		try {

			String username = request.getParameter("username");
			String password = request.getParameter("password");
			boolean useMoviePosters = request.getParameter("useMoviePosters") == null ? false : true;

			UserDAO userDAO = new UserDAO();
			CustomerTO cto = userDAO.getCustomerByCredential(username, password);
			Date date = new Date();

			if (cto != null) {
				
				custRatingDAO.deleteCustomerRating(cto.getId());

				/////// ACTIVITY ////////
				ActivityTO activityTO = new ActivityTO();
				activityTO.setActivity(ActivityType.LOGIN);
				activityTO.setCustId(cto.getId());
				ActivityDAO aDAO = new ActivityDAO();
				aDAO.insertCustomerActivity(activityTO);

				activityTO.setActivity(ActivityType.LIST_MOVIES);
				aDAO.insertCustomerActivity(activityTO);

				HttpSession session = request.getSession();
				session.setAttribute("username", username);
				session.setAttribute("time", date);
				session.setAttribute("userId", cto.getId());
				session.setAttribute("name", cto.getName());
				session.setAttribute("useMoviePosters", useMoviePosters);

				// Ashok
				System.out.println(" setting session and redirecting " + activityTO.toJsonString());
				response.sendRedirect(indexPage);

			} else {
				response.sendRedirect(loginPage + "?error=1");
			}

		} catch (Exception e) {
			e.printStackTrace();
		} // try/catch

	}
}
