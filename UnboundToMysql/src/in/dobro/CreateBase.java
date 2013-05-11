package in.dobro;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class CreateBase {
	
	static String lang = "tl";
	
	static Properties connInfo = new Properties();
	static {
		connInfo.put("characterEncoding","UTF8");
		connInfo.put("user", "root");
		connInfo.put("password", "zxasqw12");
	}

	static Connection	conn  = null; 
	static Statement st;
	static BufferedReader reader;
	static String[] strline = null;
	static String insmysql = null;
	static String insmysql2 = null;
	static PreparedStatement   countchapters 		= null;
    static ResultSet           rcountchapters    	= null;
    static Integer chapters	= null;

	public static void main(String[] args) throws SQLException, IOException {
		
		conn = DriverManager.getConnection("jdbc:mysql://localhost/bible_"+lang+"?", connInfo);
		
		st=conn.createStatement();
		//создаем базу данных
		//st.executeUpdate("CREATE DATABASE bible_"+lang);
		//создаем таблицу текста Библии
		createtexttable();
		//создаем таблицу книг Библии
		createbibletable();
		//экспортируем UnboundBible CSV в Mysql
		inserttextteable();
		//наполняем таблицу книг и глав Библии
		insertbibletable();
	}

	private static void insertbibletable() throws SQLException {
		for(int i=1;i<=66;i++) {
			countchapters = conn.prepareStatement("SELECT MAX(chapter) as maxchapter FROM `"+lang+"text` WHERE bible = " + i + ";");
			if (countchapters.execute()) {
				rcountchapters = countchapters.getResultSet();
				if(rcountchapters.next())
					chapters = rcountchapters.getInt(1);
			}
			insmysql2 = "INSERT INTO `"+lang+"bible` (`idbible`, `biblename`, `chapters`) VALUES (" +
					i + ",\"" + names.tlbible[i-1] + "\"," + chapters + ");";
			st.execute(insmysql2);
			System.out.println(insmysql2);
		}
	}

	private static void inserttextteable() throws IOException, SQLException {
		File f = new File("./csv/"+lang+".txt");
		if (!f.canRead()) {
			throw new FileNotFoundException("Файл '" + f.getAbsolutePath() + "' - не найден!");
		} else {
			
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			String line;
			while ((line = reader.readLine()) != null) {
				strline = line.split("_");
				if(strline.length > 3) {
				insmysql = "INSERT INTO `"+lang+"text` (`bible`, `chapter`, `poem`, `poemtext`) VALUES" +
						   "("+strline[0]+","+strline[1]+","+strline[2]+",\""+strline[3]+"\");";
				st.execute(insmysql);
				System.out.println(insmysql);
				}
			}
			reader.close();
		}
	}

	private static void createbibletable() throws SQLException {
		String createbibletable = "CREATE TABLE IF NOT EXISTS `"+lang+"bible` (" + 
				"`idbible` int(11) NOT NULL," +
				"`biblename` varchar(255) DEFAULT NULL," +
				"`chapters` int(11) DEFAULT NULL," +
				"PRIMARY KEY (`idbible`)" +
				") ENGINE=MyISAM  DEFAULT CHARSET=utf8"; 
		st.executeUpdate(createbibletable);
	}

	private static void createtexttable() throws SQLException {
		String createtexttable = "CREATE TABLE IF NOT EXISTS `"+lang+"text` (" + 
				"`_id` int(255) NOT NULL AUTO_INCREMENT," +
				"`bible` int(11) NOT NULL," +
				"`chapter` int(11) DEFAULT NULL," +
				"`poem` int(11) DEFAULT NULL," +
				"`poemtext` text," +
				"PRIMARY KEY (`_id`)" +
				") ENGINE=MyISAM  DEFAULT CHARSET=utf8"; 
		st.executeUpdate(createtexttable);
	}

}
