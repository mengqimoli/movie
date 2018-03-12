package oracle.demo.oow.bd.util.hbase.loder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.dao.CustomerDAO;
import oracle.demo.oow.bd.dao.hbase.UserDAO;
import oracle.demo.oow.bd.to.CustomerTO;

/**
 * 导入用户相关数据数据 在customer.out中
 * 
 * @author Luff
 *
 */
public class UserLoder {
	
	public static void main(String[] args) {
		UserLoder userLoder = new UserLoder();
		try {
			userLoder.uploadProfile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void uploadProfile() throws IOException {
		FileReader fr = null;
		CustomerDAO custDAO = new CustomerDAO();
		UserDAO userDAO = new UserDAO();
		try {

			/**
			 * Open the customer.out file for read.
			 */
			fr = new FileReader(Constant.CUSTOMER_PROFILE_FILE_NAME);
			BufferedReader br = new BufferedReader(fr);
			String jsonTxt = null;
			// String password =
			// StringUtil.getMessageDigest(Constant.DEMO_PASSWORD);
			String password = Constant.DEMO_PASSWORD;
			CustomerTO custTO = null;
			int count = 1;

			/**
			 * Loop through the file until EOF. Save the content of each row in
			 * the jsonTxt string.
			 */
			while ((jsonTxt = br.readLine()) != null) {

				if (jsonTxt.trim().length() == 0)
					continue;

				try {
					/**
					 * Construct the CustomerTO by passing the jsonTxt as an
					 * input argument to its constructor. If the jsonTxt can be
					 * deserialized into CustomerTO then a valid object will be
					 * returned but if it fails to desiralize it for any reason
					 * the null pointer will be returned.
					 */
					custTO = new CustomerTO(jsonTxt.trim());

					// Set password to each CutomerTO
					custTO.setPassword(password);
				} catch (Exception e) {
					System.out.println("ERROR: Not able to parse the json string: \t" + jsonTxt);
				}

				/**
				 * Make sure that custTO is not null, which means the jsonTxt
				 * read from the customer.out was successfully converted into
				 * CustomerTO object.
				 */
				if (custTO != null) {

					/**
					 * Persist user-profile information into kv-store. All the
					 * Customer specific read/write operations are defined in
					 * CustomerDAO class.
					 */
					userDAO.insert(custTO);

					/**
					 * If username & password doesn't exist already in the
					 * kv-store then the new profile will be created and the
					 * 'version' object would have the Version of the new
					 * key-value pair, but if the profile already exist in the
					 * store with the same credential then null will be returned
					 * and exception can be handled appropriately.
					 */
					System.out.println("WARNING: User account for '" + custTO.getUserName()
							+ "' couldn't be created because " + "username/password combination already exist.");

				} // EOF if

			} // EOF while
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			fr.close();
		}
	}
}
