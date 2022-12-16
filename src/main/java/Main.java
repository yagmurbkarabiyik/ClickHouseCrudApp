import java.sql.*;
import java.util.*;

//AutoCloseable -> Close for URL at the end of the transaction
public class Main implements AutoCloseable{
    private static Scanner scanner = new Scanner(System.in);
    //ch:clikhouse host comes from clickHouse
    private static final String DB_URL="jdbc:ch:http://localhost:18123";
    private final Connection conn;
    private final Statement stmt;
    public Main() throws SQLException{
        conn = DriverManager.getConnection(DB_URL);
        stmt = conn.createStatement();
    }
    public static void main(String[] args) throws SQLException {

        Main clickhouse = new Main();
        System.out.println("Welcome to the products table transactions. (Press '0' for quit.)\n" +
                "1 - List Products \n" +
                "2 - Add Product \n" +
                "3 - Update Product \n" +
                "4 - Delete Product \n");
        System.out.println("****************************");
        int choice;

        do{
            System.out.print("Your choice: ");
            choice = scanner.nextInt();

            switch (choice){
                case 1:
                    clickhouse.listProducts();
                    break;
                case 2:
                    clickhouse.addProduct();
                    break;
                case 3:
                    clickhouse.listProducts();
                    System.out.println("************************************");
                    clickhouse.updateProducts();
                    System.out.println("************************************");
                    clickhouse.listProducts();
                    break;
                case 4:
                    clickhouse.listProducts();
                    System.out.println("************************************");
                    clickhouse.deleteProduct();
                    System.out.println("************************************");
                    clickhouse.listProducts();
                    break;
            }
        }while (choice != 0);
    }

    public void createTable() throws SQLException{
        try{
            stmt.execute("CREATE TABLE IF NOT EXISTS products(id Int8, productName String, unitsInStock Int8, price Int8) ENGINE = MergeTree ORDER BY id");
        }
        catch(Exception e){
            System.out.println("We have got error here!");
            System.out.println(e.getMessage());
        }
    }

    public void addProduct() throws SQLException{

        System.out.print("How many products do you want to enter?");
        int productNumber = scanner.nextInt();

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO products SELECT col1, col2, col3, col4 FROM input('col1 Int8, col2 String, col3 Int8, col4 Int8')")) {
            for (int i = 0; i < productNumber; i++) {
                System.out.print("Enter id: ");
                int id = scanner.nextInt();
                scanner.nextLine();
                System.out.print("Enter product name: ");
                String productName = scanner.nextLine();
                System.out.print("Enter units in stock: ");
                int unitsInStock = scanner.nextInt();
                System.out.print("Enter price of product: ");
                int price = scanner.nextInt();
                System.out.println("*********************");


                ps.setInt(1, id);
                ps.setString(2, productName); // col1
                ps.setInt(3, unitsInStock);
                ps.setInt(4, price);
                ps.addBatch();
            }
            ps.executeBatch();
        }
        catch (Exception e){
            //Comes from SQL
            System.out.println("We have got error here!");
            System.out.println(e.getMessage());
        }
    }

    public void updateProducts() throws SQLException{
        try{
            PreparedStatement ps = conn.prepareStatement("UPDATE products SET productName=?, unitsInStock=?, price=? WHERE id=?");
            System.out.print("Which product that you want to update? Please enter product's id: ");
            int id = scanner.nextInt();
            System.out.print("Please enter product's name: ");
            String productName = scanner.next();
            scanner.nextLine();
            System.out.print("Please enter product's units in stock: ");
            int unitsInStock = scanner.nextInt();
            System.out.print("Please enter product's price: ");
            int price = scanner.nextInt();

            ps.setString(1, productName);
            ps.setInt(2, unitsInStock);
            ps.setInt(3, price);
            ps.setInt(4, id);
            ps.executeUpdate();
        }
        catch (Exception e){
            System.out.println("We have got error here!");
            System.out.println(e.getMessage());
        }
    }

    public void deleteProduct() throws SQLException{
        try{
            PreparedStatement ps = conn.prepareStatement("DELETE FROM products WHERE id=?");
            System.out.print("Which product that you want to delete? Please enter product's id: ");
            int id = scanner.nextInt();
            ps.setInt(1, id);
            ps.executeUpdate();
        }
        catch (Exception e){
            System.out.println("We have got error here!");
            System.out.println(e.getMessage());
        }
    }

    public void listProducts() throws SQLException{
        try {
            ResultSet rs = stmt.executeQuery("SELECT * FROM products ORDER BY id");
            while (rs.next()) {
                System.out.println(String.format("id: %s productName: %s unitsInStock: %s price: %s", rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)));
            }
        }
        catch (Exception e){
            System.out.println("We have got error here!");
            System.out.println(e.getMessage());
        }
    }


    //Close for connection at the end of the transaction
    @Override
    public void close() throws Exception {
        if(conn != null) {
            conn.close();
        }
    }
}