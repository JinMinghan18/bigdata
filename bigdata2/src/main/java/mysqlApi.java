import java.sql.*;

public class mysqlApi {
    static final String DRIVER="com.mysql.cj.jdbc.Driver";
    static final String DB="jdbc:mysql://hadoop101/student";
    static final String USER="root";
    static final String PASSWD="123456";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName(DRIVER);
            System.out.println("Connection for a selected databases");
            conn = DriverManager.getConnection(DB,USER,PASSWD);
            stmt = conn.createStatement();
            String sql1 = "insert into Student values('scofield',45,89,100)";
            String sql2 = "select English from Student where Name='scofield'";
//            stmt.executeUpdate(sql1);
            PreparedStatement pstmt = conn.prepareStatement(sql2);
            ResultSet rst = pstmt.executeQuery();
            if(rst.next()!=false){
                int grade = rst.getInt("English");
                System.out.println("English score = " + grade);
            }


        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }finally {
            if(stmt!=null){
                try {
                    stmt.close();
                }catch (SQLException se){
                    se.printStackTrace();
                }
            }
        }
    }

}
