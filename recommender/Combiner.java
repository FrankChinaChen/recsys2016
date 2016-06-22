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
import java.util.Collections;
import java.util.Comparator;
import recommender.Logger;

public class Combiner {
	long startTime;
	long timeStamp;
	String fileByImpressions = "src\\result\\by_impressions.csv";
	String fileByInteractionsRefined = "src\\result\\by_interactions_refined.csv";
	String fileByInteractionsType4 = "src\\result\\by_interactions_Type4.csv";
	String fileBaseline = "src\\result\\round0.csv";
	
	File fByImpressions;
	FileInputStream fisByImpressions;
	FileChannel fcByImpressions;
	MappedByteBuffer mbbByImpressions;
	byte[] bufferByImpressions;
	ByteArrayInputStream baisByImpressions;
	InputStreamReader isrByImpressions;
	BufferedReader brByImpressions;
	File fByInteractionsRefined;
	FileInputStream fisByInteractionsRefined;
	FileChannel fcByInteractionsRefined;
	MappedByteBuffer mbbByInteractionsRefined;
	byte[] bufferByInteractionsRefined;
	ByteArrayInputStream baisByInteractionsRefined;
	InputStreamReader isrByInteractionsRefined;
	BufferedReader brByInteractionsRefined;
	File fByInteractionsType4;
	FileInputStream fisByInteractionsType4;
	FileChannel fcByInteractionsType4;
	MappedByteBuffer mbbByInteractionsType4;
	byte[] bufferByInteractionsType4;
	ByteArrayInputStream baisByInteractionsType4;
	InputStreamReader isrByInteractionsType4;
	BufferedReader brByInteractionsType4;
	FileWriter fwBaseline;
	BufferedWriter bwBaseline;
	
	String lineRead;
	ArrayList<Data> impList;
	ArrayList<Data> refinedList;
	ArrayList<Data> type4List;
	Logger logger;
	
	public Combiner () {
		init ();
		run ();
	}
	
	public static void main(String[] args) {
		ByImpressions bimp = new ByImpressions ();
		ByInteractions binter = new ByInteractions ();
		Combiner c = new Combiner ();
	}
	
	private void init () {
		startTime = System.currentTimeMillis ();
		timeStamp = startTime;
		lineRead = "";
		impList = new ArrayList<Data> ();
		refinedList = new ArrayList<Data> ();
		type4List = new ArrayList<Data> ();
		logger = new Logger (startTime);
	}
	
	private void run () {
		logger.log ("START| run");
		readByImpressions ();
		logger.logTime ("DONE| readByImpressions");
		readByInteractionsRefined ();
		logger.logTime ("DONE| readByInteractionsRefined");
		readByInteractionsType4 ();
		logger.logTime ("DONE| readByInteractionsType4");
		process ();
		logger.logTime ("DONE| process");
		writeBaseline ();
		logger.logTime ("DONE| writeBaseline");
		logger.logTimeTotal ("FINISH| run");
	}
	
	private void readByImpressions () {
		try {
			fByImpressions = new File (fileByImpressions);
			fisByImpressions = new FileInputStream (fByImpressions);
			fcByImpressions = fisByImpressions.getChannel ();
			mbbByImpressions = fcByImpressions.map (FileChannel.MapMode.READ_ONLY, 0, fcByImpressions.size ());
			bufferByImpressions = new byte[(int) fcByImpressions.size ()];
			mbbByImpressions.get (bufferByImpressions);
			baisByImpressions = new ByteArrayInputStream (bufferByImpressions);
			isrByImpressions = new InputStreamReader (baisByImpressions);
			brByImpressions = new BufferedReader (isrByImpressions);
			
			while ((lineRead = brByImpressions.readLine ()) != null) {
				impList.add (new Data (lineRead));
			}
		}
		catch (IOException e) {
			e.printStackTrace ();
		}
		finally {
			if (fisByImpressions != null) try { fisByImpressions.close (); } catch (IOException e) { e.printStackTrace (); }
			if (brByImpressions != null) try { brByImpressions.close (); } catch (IOException e) { e.printStackTrace (); }
		}
	}
	
	private void readByInteractionsRefined () {
		try {
			fByInteractionsRefined = new File (fileByInteractionsRefined);
			fisByInteractionsRefined= new FileInputStream (fByInteractionsRefined);
			fcByInteractionsRefined = fisByInteractionsRefined.getChannel ();
			mbbByInteractionsRefined = fcByInteractionsRefined.map (FileChannel.MapMode.READ_ONLY, 0, fcByInteractionsRefined.size ());
			bufferByInteractionsRefined = new byte[(int) fcByInteractionsRefined.size ()];
			mbbByInteractionsRefined.get (bufferByInteractionsRefined);
			baisByInteractionsRefined = new ByteArrayInputStream (bufferByInteractionsRefined);
			isrByInteractionsRefined = new InputStreamReader (baisByInteractionsRefined);
			brByInteractionsRefined = new BufferedReader (isrByInteractionsRefined);
			
			while ((lineRead = brByInteractionsRefined.readLine ()) != null) {
				refinedList.add (new Data (lineRead));
			}
		}
		catch (IOException e) {
			e.printStackTrace ();
		}
		finally {
			if (fisByInteractionsRefined != null) try { fisByInteractionsRefined.close (); } catch (IOException e) { e.printStackTrace (); }
			if (brByInteractionsRefined != null) try { brByInteractionsRefined.close (); } catch (IOException e) { e.printStackTrace (); }
		}
	}
	
	private void readByInteractionsType4 () {
		try {
			fByInteractionsType4 = new File (fileByInteractionsType4);
			fisByInteractionsType4 = new FileInputStream (fByInteractionsType4);
			fcByInteractionsType4 = fisByInteractionsType4.getChannel ();
			mbbByInteractionsType4 = fcByInteractionsType4.map (FileChannel.MapMode.READ_ONLY, 0, fcByInteractionsType4.size ());
			bufferByInteractionsType4 = new byte[(int) fcByInteractionsType4.size ()];
			mbbByInteractionsType4.get (bufferByInteractionsType4);
			baisByInteractionsType4 = new ByteArrayInputStream (bufferByInteractionsType4);
			isrByInteractionsType4 = new InputStreamReader (baisByInteractionsType4);
			brByInteractionsType4 = new BufferedReader (isrByInteractionsType4);
			
			while ((lineRead = brByInteractionsType4.readLine ()) != null) {
				type4List.add (new Data (lineRead));
			}
		}
		catch (IOException e) {
			e.printStackTrace ();
		}
		finally {
			if (fisByInteractionsType4 != null) try { fisByInteractionsType4.close (); } catch (IOException e) { e.printStackTrace (); }
			if (brByInteractionsType4 != null) try { brByInteractionsType4.close (); } catch (IOException e) { e.printStackTrace (); }
		}
	}
	
	private void process () {
		int Cnt = 0;
		int CntBig = 0;
		for (Data imp: impList) {
			killFour (imp);
		}
		logger.log ("PROGRESS| in process: stage1");
		for (Data imp: impList) {
			if (imp.items.size () != 30) {
				append (imp);
				Cnt++;
				if (Cnt / 10000 != CntBig) {
					CntBig++;
					logger.log ("PROGRESS| in process: stage2: (" + String.valueOf (Cnt) + "/110000)");
				}
			}
		}
		logger.log ("PROGRESS| in process: stage2: (110000/110000)");
		for (Data refined: refinedList) {
			if (!existAlready (refined)) {
				impList.add (new Data (refined));
			}
		}
		logger.log ("PROGRESS| in process: stage3");
		Collections.sort (impList, new dataAscending ());
		logger.log ("PROGRESS| in process: stage4");
	}
	
	private void killFour (Data target) {
		loop1: for (int i = 0; i < type4List.size (); i++) {
			Data current = type4List.get (i);			
			if (current.user == target.user) {
				ArrayList<Integer> temp = new ArrayList<Integer> ();
				for (Integer item: target.items) {
					if (!existFour (item, current.items)) temp.add (item);
				}
				target.setItems (temp);
				break loop1;
			}
		}
	}
	
	private boolean existFour (int item, ArrayList<Integer> four) {
		for (Integer i: four) { if (i == item) { return true; } }
		return false;
	}
	
	private void append (Data target) {
		loop1: for (int i = 0; i < refinedList.size (); i++) {
			Data current = refinedList.get (i);
			if (current.user == target.user) {
				int left = 30 - target.items.size ();
				ArrayList<Integer> currentItems = current.items;
				for (int j = 0; (j < left && j < currentItems.size ()); j++) {
					target.appendItem (currentItems.get (j));
				}
				break loop1;
			}
		}
	}
	
	private boolean existAlready (Data target) {
		for (Data imp: impList) { if (imp.user == target.user) { return true; }	}
		return false;
	}
	
	private void writeBaseline () {
		try {
			fwBaseline = new FileWriter (fileBaseline);
			bwBaseline = new BufferedWriter (fwBaseline);
			
			for (Data imp: impList) {
				bwBaseline.write (imp.toString ());
				bwBaseline.newLine ();
			}
		}
		catch (IOException e) {
			e.printStackTrace ();
		}
		finally {
			if (bwBaseline != null) try { bwBaseline.close (); } catch (IOException e) { e.printStackTrace (); }
			if (fwBaseline != null) try { fwBaseline.close (); } catch (IOException e) { e.printStackTrace (); }
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
			if (items.size () != 0) {
				int i = 0;
				for (i = 0; (i < items.size()-1 && i < 29); i++) {
					temp += String.valueOf (items.get (i)) + ",";
				}
				temp += String.valueOf (items.get (i));
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
	
	static class dataAscending implements Comparator<Data> {
		@Override
		public int compare (Data arg0, Data arg1) {
			if (arg0.user < arg1.user) return -1;
			else if (arg0.user > arg1.user) return 1;
			else return 0;
		}
	}
}
