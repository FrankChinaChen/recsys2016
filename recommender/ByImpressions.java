package recommender;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import recommender.Logger;

public class ByImpressions {
	public static final int ITEMS_EACH = 30;
	long startTime;
	long timeStamp;
	String fileUsers = "src\\target_users.csv";
	String fileImpressions = "src\\impressions.csv";
	String fileByImpressions = "src\\result\\by_impressions.csv";
	
	File fUsers;
	FileInputStream fisUsers;
	FileChannel fcUsers;
	MappedByteBuffer mbbUsers;
	byte[] bufferUsers;
	ByteArrayInputStream baisUsers;
	InputStreamReader isrUsers;
	BufferedReader brUsers;
	FileReader frImpressions;
	BufferedReader brImpressions;
	FileWriter fwByImpressions;
	BufferedWriter bwByImpressions;
	
	String lineRead;
	ArrayList<Integer> userList;
	ArrayList<Impression> impList;
	Logger logger;
	
	public ByImpressions () {
		init ();
		run ();
	}
	
	public static void main(String[] args) {
		ByImpressions bimp = new ByImpressions ();
	}
	
	public void init () {
		startTime = System.currentTimeMillis ();
		timeStamp = startTime;
		lineRead = "";
		userList  = new ArrayList<Integer> ();
		impList = new ArrayList<Impression> ();
		logger = new Logger (startTime);
	}
	
	public void run () {
		logger.log ("START| run");
		readUsers ();
		logger.logTime ("DONE| readUsers");
		readMostRecent ();
		logger.logTime ("DONE| pickMostRecent");
		pickTarget ();
		logger.logTime ("DONE| pickTarget");
		writeByImpressions ();
		logger.logTime ("DONE| writeByImpressions");
		logger.logTimeTotal ("FINISH| run");
	}
	
	public void readUsers () {
		try {
			fUsers = new File (fileUsers);
			fisUsers = new FileInputStream (fUsers);
			fcUsers = fisUsers.getChannel ();
			mbbUsers = fcUsers.map (FileChannel.MapMode.READ_ONLY, 0, fcUsers.size ());
			bufferUsers = new byte[(int) fcUsers.size ()];
			mbbUsers.get (bufferUsers);
			baisUsers = new ByteArrayInputStream (bufferUsers);
			isrUsers = new InputStreamReader (baisUsers);
			brUsers = new BufferedReader (isrUsers);
			
			lineRead = brUsers.readLine ();
			while ((lineRead = brUsers.readLine ()) != null) {
				userList.add (Integer.parseInt (lineRead));
			}
		}
		catch (IOException e) {
			e.printStackTrace ();
		}
		finally {
			if (fisUsers != null) try { fisUsers .close (); } catch (IOException e) { e.printStackTrace (); }
			if (brUsers != null) try { brUsers.close (); } catch (IOException e) { e.printStackTrace (); }
		}
	}
	
	public void readMostRecent () {
		try {
			Impression current = new Impression ();
			Impression imp = new Impression ();
			frImpressions = new FileReader (fileImpressions);
			brImpressions = new BufferedReader (frImpressions);
			lineRead = brImpressions.readLine ();
			while ((lineRead = brImpressions.readLine ()) != null) {
				imp = new Impression (lineRead);
				if (imp.user == current.user) {
					if (imp.week > current.week) current = new Impression (imp);
				}
				else {
					if (current.user != 0) { impList.add (new Impression (current)); }
					current = new Impression (imp);
				}
			}
			impList.add (new Impression (current));
			Collections.sort (impList, new impressionAscending ());
		}
		catch (IOException e) {
			e.printStackTrace ();
		}
		finally {
			if (brImpressions != null) try { brImpressions.close (); } catch (IOException e) { e.printStackTrace (); }
			if (frImpressions != null) try { frImpressions.close (); } catch (IOException e) { e.printStackTrace (); }
		}
	}
	
	public void pickTarget () {
		int Cnt = 0;
		int CntBig = 0;
		ArrayList<Impression> temp = new ArrayList<Impression> ();
		
		for (Impression imp: impList) {
			if (isTarget (imp)) {
				temp.add (imp);
				Cnt++;
				if (Cnt / 10000 != CntBig) {
					CntBig++;
					logger.log ("PROGRESS| in pickTarget: (" + String.valueOf (Cnt) + "/140000)");
				}
			}
			
		}
		logger.log ("PROGRESS| in pickTarget: (140000/140000)");
		impList = new ArrayList<Impression> (temp);
	}
	
	public boolean isTarget (Impression imp) {
		for (Integer user: userList) {
			if (user == imp.user) return true;
		}
		return false;
	}
	
	public void writeByImpressions () {
		try {
			fwByImpressions = new FileWriter (fileByImpressions);
			bwByImpressions = new BufferedWriter (fwByImpressions);
			for (Impression imp: impList) {
				bwByImpressions.write (imp.toString ());
				bwByImpressions.newLine ();
			}
		}
		catch (IOException e) {
			e.printStackTrace ();
		}
		finally {
			if (bwByImpressions != null) try { bwByImpressions.close (); } catch (IOException e) { e.printStackTrace (); }
			if (fwByImpressions != null) try { fwByImpressions.close (); } catch (IOException e) { e.printStackTrace (); }
		}
	}
	
	class Impression {
		int user;
		int year;
		int week;
		String items;
		
		Impression () {
			user = 0;
			year = 0;
			week = 0;
			items = "";
		}
		
		Impression (String str) {
			String[] temp = null;
			temp = str.split ("\t", 4);
			user = Integer.parseInt (temp[0]);
			year = Integer.parseInt (temp[1]);
			week = Integer.parseInt (temp[2]);
			items = temp[3];
		}
		
		Impression (Impression another) {
			user = another.user;
			year = another.year;
			week = another.week;
			items = new String (another.items);
		}
		@Override
		public String toString () {
			String result = "";
			result += String.valueOf (user) + "\t";
			result += duplicateKiller (items);
			return result;
		}
		
		private String duplicateKiller (String items) {
			String[] temp = null;
			ArrayList<String> intermediate = new ArrayList<String> ();
			String result = "";
			int resultCount = 0;
			
			temp = items.split (",");
			for (int i = 0; i < temp.length; i++) {
				if (!isDuplicate (temp[i], intermediate)) intermediate.add (temp[i]);
			}
			
			loop: for (int i = 0; i < intermediate.size (); i++) {
				result += intermediate.get (i);
				resultCount++;
				if (resultCount == ITEMS_EACH) break loop; // limit items
				if (i != intermediate.size () - 1) { result += ","; }
			}
			return result;
		}
		
		private boolean isDuplicate (String item, ArrayList<String> intermediate) {
			for (int i = 0; i < intermediate.size (); i++) {
				if (intermediate.get (i).equals (item)) return true;
			}
			return false;
		}
	}
	
	static class impressionAscending implements Comparator<Impression> {
		@Override
		public int compare (Impression arg0, Impression arg1) {
			if (arg0.user < arg1.user) return -1;
			else if (arg0.user > arg1.user) return 1;
			else return 0;
		}
	}
}
