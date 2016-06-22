package recommender;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import recommender.Logger;

public class SimpleWalk {
	static final int ROUND = 3;
	static final double THRESHOLD = 0.50;
	long startTime;
	long timeStamp;
	//String filePrevious= "src\\sample\\tester.csv";
	//String fileNext = "src\\sample\\testerSol.csv";
	//String filePrevious = "src\\result\\round" + String.valueOf (ROUND-1) + ".csv";
	String filePrevious = "src\\result\\round" + String.valueOf (ROUND-1) +"_THRESHOLD" + String.valueOf(THRESHOLD * 100) + ".csv";
	String fileNext = "src\\result\\round" + String.valueOf (ROUND) +"_THRESHOLD" + String.valueOf(THRESHOLD * 100) + ".csv";
	
	File fPrevious;
	FileInputStream fisPrevious;
	FileChannel fcPrevious;
	MappedByteBuffer mbbPrevious;
	byte[] bufferPrevious;
	ByteArrayInputStream baisPrevious;
	InputStreamReader isrPrevious;
	BufferedReader brPrevious;
	FileWriter fwNext;
	BufferedWriter bwNext;
	
	String lineRead;
	ArrayList<Data> prevList;
	ArrayList<Data> pickList;
	ArrayList<Data> nextList;
	Logger logger;

	public SimpleWalk () {
		init ();
		run ();
	}
	
	public static void main(String[] args) {
		SimpleWalk sw = new SimpleWalk ();
	}
	
	private void init () {
		startTime = System.currentTimeMillis ();
		timeStamp = startTime;
		lineRead = "";
		prevList = new ArrayList<Data> ();
		pickList = new ArrayList<Data> ();
		nextList = new ArrayList<Data> ();
		logger = new Logger (startTime);
	}
	
	private void run () {
		logger.log ("START| run");
		readPrevious ();
		logger.logTime ("DONE| readPrevious");
		//loggerList ();
		pick ();
		logger.logTime ("DONE| pick");
		//loggerList ();
		process ();
		logger.logTime ("DONE| process");
		//loggerList ();
		writeNext ();
		logger.logTime ("DONE| writeNext");
		//loggerList ();
		logger.logTimeTotal ("FINISH| run");
	}
	
	private void readPrevious () {
		try {
			fPrevious = new File (filePrevious);
			fisPrevious = new FileInputStream (fPrevious);
			fcPrevious = fisPrevious.getChannel ();
			mbbPrevious = fcPrevious.map (FileChannel.MapMode.READ_ONLY, 0, fcPrevious.size());
			bufferPrevious = new byte[(int) fcPrevious.size()];
			mbbPrevious.get (bufferPrevious);
			baisPrevious = new ByteArrayInputStream (bufferPrevious);
			isrPrevious = new InputStreamReader (baisPrevious);
			brPrevious = new BufferedReader (isrPrevious);
			
			while ((lineRead = brPrevious.readLine ()) != null) {
				prevList.add (new Data (lineRead));
			}
		}
		catch (IOException e) {
			e.printStackTrace ();
		}
		finally {
			if (fisPrevious != null) try { fisPrevious.close (); } catch (IOException e) { e.printStackTrace (); }
			if (brPrevious != null) try { brPrevious.close (); } catch (IOException e) { e.printStackTrace (); }
		}
	}
	
	private void pick () {
		int Cnt = 0;
		int CntBig = 0;
		for (Data prev: prevList) {
			findSimilar (prev);
			Cnt++;
			if (Cnt / 100 != CntBig) {
				CntBig++;
				logger.logTime ("PROGRESS| in pick: (" + String.valueOf (Cnt) + "/135500)");
			}
		}
		logger.log ("PROGRESS| in pick: (135500/135500)");
	}
	
	private void findSimilar (Data target) {
		Data similar = new Data ();
		double scoreMax = 0;
		double scoreCurrent = 0;
		for (Data prev: prevList) {
			if (prev.user != target.user) {
				scoreCurrent = calculateScore (target, prev);
				if (scoreCurrent >= THRESHOLD) {
					if (scoreCurrent > scoreMax) {
						scoreMax = scoreCurrent;
						similar.setBy (prev);
					}
					else if ((scoreCurrent > scoreMax*0.9) && (((float) prev.items.size()/similar.items.size()) > 1.1)) {
						scoreMax = scoreCurrent;
						similar.setBy (prev);
					}
				}
			}
		}
		//if (similar.user != 0) logger.log ("score: " + String.valueOf (scoreMax) + ": " + similar.user + " / " + target.user);
		pickList.add (similar);
	}
	
	private double calculateScore (Data target, Data candidate) {
		int numer = 0;
		int denom = target.items.size();
		
		if (denom == 0) return 0;
		else {
			loop1: for (Integer canItem: candidate.items) {
				loop2: for (Integer tarItem: target.items) {
					if (tarItem.equals (canItem)) {
						numer++;
						break loop2;
					}
				}
			}
		}
		return (double) numer / denom;
	}
	
	private void process () {
		for (int i = 0; i < prevList.size(); i++) {
			Data target = prevList.get(i);
			Data picked = pickList.get(i);
			if (picked.user == 0) { nextList.add (target); }
			else {
				Data temp = new Data (target);
				int left = 30 - temp.items.size();
				for (int j = 0; (j < left && j < picked.items.size()); j++) {
					temp.appendItem (picked.items.get(j));
				}
				nextList.add (temp);
				//logger.log ("---");
				//logger.log (prevList.get(i).toString());
				//logger.log (pickList.get(i).toString());
				//logger.log (nextList.get(i).toString());
			}
		}
	}
	
	private void writeNext () {		
		try {
			fwNext = new FileWriter (fileNext);
			bwNext = new BufferedWriter (fwNext);
			
			for (Data next: nextList) {
				bwNext.write (next.toString ());
				bwNext.newLine ();
			}
		}
		catch (IOException e) {
			e.printStackTrace ();
		}
		finally {
			if (bwNext != null) try { bwNext.close (); } catch (IOException e) { e.printStackTrace (); }
			if (fwNext != null) try { fwNext.close (); } catch (IOException e) { e.printStackTrace (); }
		}
	}
	
	private void loggerList () {
		logger.log ("prevList:");
		for (Data prev: prevList) {
			logger.log ("|" + prev.toString ());
		}
		logger.log ("pickList:");
		for (Data pick: pickList) {
			logger.log ("|" + pick.toString ());
		}
		logger.log ("nextList:");
		for (Data next: nextList) {
			logger.log ("|" + next.toString ());
		}
	}
	
	class Data {
		int user;
		ArrayList<Integer> items;
		
		public Data () {
			user = 0;
			items = new ArrayList<Integer> ();
		}
		
		public Data (String str) {
			String[] temp;
			temp = str.split ("\t", 2);
			user = Integer.parseInt (temp[0]);
			items = new ArrayList<Integer> ();
			if (!temp[1].equals ("")) {
				temp = temp[1].split (",");
				if (temp.length != 0) {
					for (int i = 0; (i < temp.length && i < 30); i++) {
						items.add (Integer.parseInt (temp[i]));
					}
				}
			}
		}
		
		public Data (Data d) {
			user = d.user;
			items = new ArrayList<Integer> (d.items);
		}
		@Override
		public String toString () {
			String temp = String.valueOf (user) + "\t";
			if (items.size() != 0) {
				int i = 0;
				for (i = 0; (i < items.size()-1 && i < 29); i++) {
					temp += String.valueOf (items.get(i)) + ",";
				}
				temp += String.valueOf (items.get(i));
			}
			return temp;
		}
		
		public void setBy (Data d) {
			user = d.user;
			items = new ArrayList<Integer> (d.items);
		}
		
		public void setItems (ArrayList<Integer> _items) {
			items = new ArrayList<Integer> (_items);
		}
		
		public void appendItem (int _item) {
			if (!isDuplicate (_item)) items.add (_item);
		}
		
		public boolean isDuplicate (int _item) {
			for (Integer item: items) {	if (item == _item) { return true; }}
			return false;
		}
	}
}
