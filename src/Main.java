import java.io.IOException;
import java.sql.*;


public class Main {
    static Connection connection = null;

    public static void loadStageTable(String FileName, Connection connection) {
        String values[] = null;
        PreparedStatement pst = null;
        int i = 0;
        try {
            //Load data file will load data from file into stage table
            
            String q1 = "LOAD DATA LOCAL INFILE '"+FileName+"' INTO TABLE stage_cust FIELDS TERMINATED BY '|' \r\n"
            		+ "lines terminated by '\\r\\n' IGNORE 1 LINES (@col1,@col2,@col3,@col4,@col5,@col6,@col7,@col8,@col9,@col10,@col11,@col12) \r\n"
            		+ "set Customer_Name=@col3,Customer_Id=@col4,Customer_Open_Date=@col5,Last_Consulted_Date=@col6,Vaccination_Type=@col7,\r\n"
            		+ "Doctor_Consulted=@col8,State=@col9,Country=@col10,Date_of_Birth=STR_TO_DATE(@col11,'%d%m%Y'),Active_Customer =@col12;\r\n";
             
            
            pst = connection.prepareStatement(q1);


            pst.executeUpdate();
            System.out.println("DATA LOADED");

        } catch (Exception ex) {
            System.out.println("load error:" + ex.getMessage());
            ex.printStackTrace();

        }


    }

    public static void createCoreTables(Connection con) {
    	try {
    		System.out.println("In Core");
    		String q1 = "select distinct(Country) from stage_cust;";
    		Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(q1);
            Statement st1 = con.createStatement();
            while(rs.next()) {
                
                //Create table if not present in database according to country name
                
            	 String table_name = "Table_"+rs.getString(1);
            	 q1 = "create table IF NOT EXISTS "+table_name+"(Customer_Name varchar(255) not null primary key,Customer_Id varchar(18) not null, Customer_Open_Date date not null,\r\n"
            	 		+ "Last_Consulted_Date date, Vaccination_Type varchar(5), Doctor_Consulted varchar(255), State varchar(5), Country varchar(5), Post_Code integer(5),\r\n"
            	 		+ "Date_of_Birth date, Active_Customer varchar(1) );";
            	 //System.out.println(rs.getString(1));
            	 st1.execute(q1);
            }
            
    	}catch(Exception e) {
    		System.out.println("Exception message : " + e.getMessage());
    	}
    }

    
    public static void stageToCore(Connection con) {
    	try {
            
            // Load data to respective table
    		
    		String q1 = "select * from stage_cust;";
    		Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(q1);
            PreparedStatement pst = null;
            while(rs.next()) {
            	String table_name = "Table_"+rs.getString(8);
            	
            	pst = con.prepareStatement("insert into "+table_name+" values(?,?,STR_TO_DATE(?,'%Y-%m-%d'),STR_TO_DATE(?,'%Y-%m-%d'),?,?,?,?,?,STR_TO_DATE(?,'%Y-%m-%d'),?)");
            	for(int i=1;i<=11;i++)
            	pst.setString(i,rs.getString(i) );
            	
            	pst.executeUpdate();
            	System.out.println(pst);
            }
            
            
    	}catch(Exception e) {
    		System.out.println("Exception message : " + e.getMessage());
    	}
    }

    public static void main(String[] args) throws IOException {

        String FileName = "C://Users//Himansh//Desktop//Assg//File.txt";
        

        try {
        	Class.forName("com.mysql.cj.jdbc.Driver");
        	Connection con=DriverManager.getConnection(  
        			"jdbc:mysql://localhost:3306/test","root","****");
        	loadStageTable(FileName,con);
        	createCoreTables(con);
        	stageToCore(con);
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("Exception message : " + e.getMessage());
        }
    }


}
