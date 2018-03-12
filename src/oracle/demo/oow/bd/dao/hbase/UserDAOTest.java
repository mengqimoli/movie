package oracle.demo.oow.bd.dao.hbase;

import oracle.demo.oow.bd.to.CustomerTO;

public class UserDAOTest {
	public static void main(String[] args) {
		UserDAO userDAO = new UserDAO();
		CustomerTO customerTO = userDAO.getCustomerByCredential("guest1", "welcome1");
		System.out.println(customerTO.getUserName() + "..." + customerTO.getPassword());
	}
}
