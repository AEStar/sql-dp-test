package examples;
import java.sql.*;
import java.util.Properties;

public class CreateDBExample {
    public static void main(String[] args) {

        Properties myProp = new Properties();
        myProp.put("user", "dbadmin");
        myProp.put("password", "zhanxuchang159");

        //Set streamingBatchInsert to True to enable streaming mode for batch inserts.
        //myProp.put("streamingBatchInsert", "True");

        Connection conn;
        try {
            conn = DriverManager.getConnection(
                    "jdbc:vertica://127.0.0.1:5433/test",
                    myProp);
            // establish connection and make a table for the data.
            Statement stmt = conn.createStatement();


            // Set AutoCommit to false to allow Vertica to reuse the same
            // COPY statement
            conn.setAutoCommit(false);

            /*  used to create orders table
            // Drop table and recreate.
            stmt.execute("DROP TABLE IF EXISTS orders CASCADE");
            stmt.execute("CREATE TABLE orders (order_id int, order_date"
                    + " date, customer_id int, product_id int, "
                    + "order_cost int)");

            // Create the prepared statement
            PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO orders (order_id, order_date, " +
                            "customer_id, product_id, order_cost)" +
                            " VALUES(?,?,?,?,?)");
            // Add rows to a batch in a loop. Each iteration adds a
            // new row.
            int m = 1;
            for(int j = 0; j < 1000; j++){
                for (int i = 0; i < 1000; i++) {
                    // Add each parameter to the row.q
                    pstmt.setInt(1, m);
                    pstmt.setTimestamp(2, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
                    pstmt.setInt(3, (int) (Math.random()*100));
                    pstmt.setInt(4, (int) (Math.random()*500));
                    pstmt.setInt(5, (int) (Math.random()*10000));
                    // Add row to the batch.
                    m++;
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            */

            //used to create customers table
            // Drop table and recreate.
            stmt.execute("DROP TABLE IF EXISTS customers CASCADE");
            stmt.execute("CREATE TABLE customers (customer_id int, name char(20), address char(25))");

            // Create the prepared statement
            PreparedStatement pstmt = conn.prepareStatement("INSERT INTO customers (customer_id, name, address)" + " VALUES(?,?,?)");

            int m = 1;
            for(int j = 0; j < 1000; j++){
                for (int i = 0; i < 1000; i++) {
                    // Add each parameter to the row.q
                    pstmt.setInt(1, m);
                    pstmt.setString(2, getAlphaNumericString((int) (Math.random()*10+5)));
                    pstmt.setString(3, getAlphaNumericString((int) (Math.random()*12+10)));

                    // Add row to the batch.
                    m++;
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            // Commit the transaction to close the COPY command
            conn.commit();


            /*
            // Print the resulting table.
            ResultSet rs = null;
            rs = stmt.executeQuery("SELECT CustID, First_Name, "
                    + "Last_Name FROM customers ORDER BY CustID");
            while (rs.next()) {
                System.out.println(rs.getInt(1) + " - "
                        + rs.getString(2).trim() + " "
                        + rs.getString(3).trim());
            }
            */

            // Cleanup
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

    static String getAlphaNumericString(int n)
    {

        // chose a Character random from this String
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "abcdefghijklmnopqrstuvxyz";

        // create StringBuffer size of AlphaNumericString
        StringBuilder sb = new StringBuilder(n);

        for (int i = 0; i < n; i++) {

            // generate a random number between
            // 0 to AlphaNumericString variable length
            int index
                    = (int)(AlphaNumericString.length()
                    * Math.random());

            // add Character one by one in end of sb
            sb.append(AlphaNumericString
                    .charAt(index));
        }

        return sb.toString();
    }

}

