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

public class ByInteractions {
	long startTime;
	long timeStamp;
	String fileUsers = "src\\target_users.csv";
	String fileInteractions = "src\\interactions.csv";
	String fileByInteractionsRefined = "src\\result\\by_interactions_refined.csv";
	String fileByInteractionsType4 = "src\\result\\by_interactions_type4.csv";
	
	File fUsers;
	FileInputStream fisUsers;
	FileChannel fcUsers;
	MappedByteBuffer mbbUsers;
	byte[] bufferUsers;
	ByteArrayInputStream baisUsers;
	InputStreamReader isrUsers;
	BufferedReader brUsers;
	File fInteractions;
	FileInputStream fisInteractions;
	FileChannel fcInteractions;
	MappedByteBuffer mbbInteractions;
	byte[] bufferInteractions;
	ByteArrayInputStream baisInteractions;
	InputStreamReader isrInteractions;
	BufferedReader brInteractions;
	FileWriter fwByInteractionsRefined;
	BufferedWriter bwByInteractionsRefined;
	FileWriter fwByInteractionsType4;
	BufferedWriter bwByInteractionsType4;
	
	String lineRead;
	ArrayList<Integer> userList;
	ArrayList<Interaction> interList;
	ArrayList<PerUser> perUserList;
	ArrayList<PerUser> refinedList;
	ArrayList<PerUser> type4List;
	Logger logger;
	
	public ByInteractions () {
		init ();
		run ();
	}
	
	public static void main(String[] args) {
		ByInteractions binter = new ByInteractions ();
	}
	
	private void init () {
		startTime = System.currentTimeMillis ();
		timeStamp = startTime;
		lineRead = "";
		userList = new ArrayList<Integer> ();
		interList = new ArrayList<Interaction> ();
		perUserList = new ArrayList<PerUser> ();
		refinedList = new ArrayList<PerUser> ();
		type4List = new ArrayList<PerUser> ();
		logger = new Logger (startTime);
	}
	
	private void run () {
		logger.log ("START| run");
		readUsers ();
		logger.logTime ("DONE| readUsers");
		readInteractions ();
		logger.logTime ("DONE| readInteractions");
		group ();
		logger.logTime ("DONE| group");
		process ();
		logger.logTime ("DONE| process");
		writeByInteractions ();
		logger.logTime ("DONE| writeByInteractions");
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
	
	public void readInteractions () {
		try {
			fInteractions = new File (fileInteractions);
			fisInteractions = new FileInputStream (fInteractions);
			fcInteractions = fisInteractions.getChannel ();
			mbbInteractions = fcInteractions.map (FileChannel.MapMode.READ_ONLY, 0, fcInteractions.size ());
			bufferInteractions = new byte[(int) fcInteractions.size ()];
			mbbInteractions.get (bufferInteractions);
			baisInteractions = new ByteArrayInputStream (bufferInteractions);
			isrInteractions = new InputStreamReader (baisInteractions);
			brInteractions = new BufferedReader (isrInteractions);
			
			lineRead = brInteractions.readLine ();
			while ((lineRead = brInteractions.readLine ()) != null) {
				interList.add (new Interaction (lineRead));
			}
		}
		catch (IOException e) {
			e.printStackTrace ();
		}
		finally {
			if (fisInteractions != null) try { fisInteractions .close (); } catch (IOException e) { e.printStackTrace (); }
			if (brInteractions != null) try { brInteractions.close (); } catch (IOException e) { e.printStackTrace (); }
		}
	}
	
	private void group () {
		int currentUser = 0;
		PerUser currentPerUser = new PerUser ();
		
		for (Interaction inter: interList) {
			if (currentUser == inter.user) {
				currentPerUser.appendInteraction (inter);
			}
			else {
				if (currentUser != 0) { perUserList.add (currentPerUser); }
				currentUser = inter.user;
				currentPerUser = new PerUser (inter);
			}
		}
		perUserList.add (currentPerUser);
		Collections.sort (perUserList, new perUserAscending ());
	}
	
	private void process () {
		int Cnt = 0;
		int CntBig = 0;

		for (PerUser pu: perUserList) {
			if (isTarget (pu)) {
				refinedList.add (new PerUser(pu));
				type4List.add (new PerUser(pu));
			}
			Cnt++;
			if (Cnt / 100000 != CntBig) {
				CntBig++;
				logger.log ("PROGRESS| in process: stage1: (" + String.valueOf (Cnt) + "/800000)");
			}
		}
		logger.log ("PROGRESS| in process: stage1: (800000/800000)");

		for (PerUser pu: refinedList) {
			pu.refine ();
		}
		logger.log ("PROGRESS| in process: stage2");

		for (PerUser pu: type4List) {
			pu.type4 ();
		}
		logger.log ("PROGRESS| in process: stage3");
		
		refinedList.removeIf (p -> p.interactions.size () == 0);
		type4List.removeIf (p -> p.interactions.size () == 0);
		logger.log ("PROGRESS| in process: stage4");
	}
	
	private boolean isTarget (PerUser pu) {
		for (Integer user: userList) {
			if (user == pu.user) return true;
		}
		return false;
	}
	
	private void writeByInteractions () {
		try {
			fwByInteractionsRefined = new FileWriter (fileByInteractionsRefined);
			bwByInteractionsRefined = new BufferedWriter (fwByInteractionsRefined);
			fwByInteractionsType4 = new FileWriter (fileByInteractionsType4);
			bwByInteractionsType4 = new BufferedWriter (fwByInteractionsType4);
			
			for (PerUser pu: refinedList) {
				bwByInteractionsRefined.write (pu.toString ());
				bwByInteractionsRefined.newLine ();
			}
			for (PerUser pu: type4List) {
				bwByInteractionsType4.write (pu.toString ());
				bwByInteractionsType4.newLine ();
			}
		}
		catch (IOException e) {
			e.printStackTrace ();
		}
		finally {
			if (bwByInteractionsRefined != null) try { bwByInteractionsRefined.close (); } catch (IOException e) { e.printStackTrace (); }
			if (fwByInteractionsRefined != null) try { fwByInteractionsRefined.close (); } catch (IOException e) { e.printStackTrace (); }
			if (bwByInteractionsType4 != null) try { bwByInteractionsType4.close (); } catch (IOException e) { e.printStackTrace (); }
			if (fwByInteractionsType4 != null) try { fwByInteractionsType4.close (); } catch (IOException e) { e.printStackTrace (); }
		}
	}
	
	class Interaction {
		public int user;
		public int item;
		public int type;
		public int created;
		
		public Interaction () {
			user = 0;
			item = 0;
			type = 0;
			created = 0;
		}
		
		public Interaction (String data) {
			String[] temp;
			temp = data.split ("\t", 4);
			user = Integer.parseInt (temp[0]);
			item = Integer.parseInt (temp[1]);
			type = Integer.parseInt (temp[2]);
			created = Integer.parseInt (temp[3]);
		}
		
		public Interaction (Interaction inter) {
			user = inter.user;
			item = inter.item;
			type = inter.type;
			created = inter.created;
		}
		
		public void setBy (Interaction inter) {
			user = inter.user;
			item = inter.item;
			type = inter.type;
			created = inter.created;
		}
	}
	
	class PerUser {
		int user;
		ArrayList<Interaction> interactions;
		
		public PerUser () {
			user = 0;
			interactions = new ArrayList<Interaction> ();
		}
		
		public PerUser (Interaction inter) {
			user = inter.user;
			interactions = new ArrayList<Interaction> ();
			interactions.add (inter);
		}
		
		public PerUser (PerUser pu) {
			user = pu.user;
			interactions = new ArrayList<Interaction> (pu.interactions);
		}
		
		public void appendInteraction (Interaction inter) {
			interactions.add (inter);
		}
		@Override
		public String toString () {
			String temp = "";
			temp += String.valueOf (user) + "\t";
			if (interactions.size () != 0) {
				int i = 0;
				for (i = 0; i < interactions.size () - 1; i++) {
					temp += String.valueOf (interactions.get (i).item) + ",";
				}
				temp += String.valueOf (interactions.get (i).item);
			}
			return temp;
		}
		
		public void refine () {
			ArrayList<Interaction> temp = new ArrayList<Interaction> ();			
			for (Interaction inter: interactions) {
				boolean exists = false;
				for (Interaction interTemp: temp) {
					if (interTemp.item == inter.item) {
						exists = true;
						if (interTemp.type < inter.type) interTemp.setBy (inter);
					}
				}
				if (!exists) temp.add (inter);
			}
			
			ArrayList<Interaction> tempNo4 = new ArrayList<Interaction> ();
			for (Interaction inter: temp) {
				if (inter.type != 4) tempNo4.add (inter);
			}
			Collections.sort (tempNo4, new interTypeDescending ());
			
			temp = new ArrayList<Interaction> ();
			for (int i = 0; (i < tempNo4.size () && i < 30); i++) {
				temp.add (tempNo4.get (i));
			}
			interactions = new ArrayList<Interaction> (temp);
		}
		
		public void type4 () {
			ArrayList<Interaction> temp = new ArrayList<Interaction> ();			
			for (Interaction inter: interactions) {
				boolean exists = false;
				for (Interaction interTemp: temp) {
					if (interTemp.item == inter.item) {
						exists = true;
						if (interTemp.type < inter.type) interTemp.setBy (inter);
					}
				}
				if (!exists) temp.add (inter);
			}
			
			ArrayList<Interaction> tempType4 = new ArrayList<Interaction> ();
			for (Interaction inter: temp) {
				if (inter.type == 4) tempType4.add (inter);
			}
			Collections.sort (tempType4, new interTypeDescending ());
			
			temp = new ArrayList<Interaction> ();
			for (int i = 0; (i < tempType4.size () && i < 30); i++) {
				temp.add (tempType4.get (i));
			}
			interactions = new ArrayList<Interaction> (temp);
		}
	}
	
	static class interTypeDescending implements Comparator<Interaction> {
		@Override
		public int compare (Interaction arg0, Interaction arg1) {
			if (arg0.type < arg1.type) return 1;
			else if (arg0.type > arg1.type) return -1;
			else return 0;
		}
	}
	
	static class perUserAscending implements Comparator<PerUser> {
		@Override
		public int compare (PerUser arg0, PerUser arg1) {
			if (arg0.user < arg1.user) return -1;
			else if (arg0.user > arg1.user) return 1;
			else return 0;
		}
	}
}
