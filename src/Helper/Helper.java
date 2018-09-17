package Helper;

import java.security.*;

public class Helper {
	public static String getMD5String(String input) {
		byte[] bytesOfMessage = input.getBytes();
		byte[] thedigest = null;
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			thedigest = md.digest(bytesOfMessage);
		} catch (Exception e) {
			System.out.println("FATAL: MD5 not supported");
			System.exit(0);
		}
		String ret = "";
		for (byte b : thedigest) {
			ret = ret + String.format("%02X", b);
		}
		return ret;
	}
}
