package bharati.binita.storm.trident.util;

import org.slf4j.Logger;

public class CommonUtil {
	
	public static void logMessage(Logger logger, String currentThreadName,
			String msgFormat, Object... args)
	{
	
		logger.info(currentThreadName +"[INFO] " + String.format(msgFormat, args));
	}

}
