import java.sql.SQLException;

public class Application {
    public static void main(String[] args){
        Comparison comparison = new Comparison();
        try{
            System.out.println(comparison.Compr(
                    "jdbc:postgresql://localhost:5432/first_compr_db",
                    "jdbc:postgresql://localhost:5432/second_compr_db",
                    "addresses",
                    "addresses",
                    "address,city"));
        }
        catch (SQLException exc){
            exc.printStackTrace();
        }
    }
}
