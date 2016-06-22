package recommender;

public class Logger {
	long startTime;
	long timeStamp;
	
	public Logger (long _startTime) {
		startTime = _startTime;
		timeStamp = startTime;
	}
	public void log (String log) {
		System.out.println (System.currentTimeMillis () / 1000 + ": " + log);
	}
	
	public void logTime (String log) {
		long current = System.currentTimeMillis ();
		float elapsed = (float) (current - timeStamp) / 1000;
		timeStamp = current;
		System.out.println (current / 1000 + ": " + log + " (elapsed: " + elapsed + "sec)");
	}
	
	public void logTimeTotal (String log) {
		long current = System.currentTimeMillis ();
		float elapsed = (float) (current - startTime) / 1000;
		System.out.println (current / 1000 + ": " + log + " (elapsedTotal: " + elapsed + "sec)");
	}
}
