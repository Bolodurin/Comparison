
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

public class Comparison{
    private String[] field;

    private int getSize(ResultSet resultSet) throws Exception{
        resultSet.last();
        int res = resultSet.getRow();
        resultSet.beforeFirst();
        return res;
    }

    private List doList(ResultSet resultSet, int size){
        List<Map<String, String>> list = new ArrayList<Map<String, String>>();

        IntStream stream1 = IntStream.range(0, size);
        stream1.forEach(s->{
            try {
                if (resultSet.next()){
                    Map res = new HashMap<String, String>();

                    IntStream streamCol = IntStream.range(0, field.length);

                    streamCol.forEach(k ->{
                        try {
                            res.put(field[k], resultSet.getString(field[k]));
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    });
                    list.add(res);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

        });
        return list;
    }

    public boolean Compr(String connect_data1,
                         String connect_data2,
                         String name_table1,
                         String name_table2,
                         String fields)
            throws SQLException{
        Connection connect1 = null;
        Connection connect2 = null;
        try{
            Class.forName("org.postgresql.Driver");
            connect1 = DriverManager.getConnection(
                    connect_data1,
                    "postgres", "1234"
            );

            if(connect1 == null){
                System.out.println("Not connected");
                System.exit(1);
            }

            connect2 = DriverManager.getConnection(
                    connect_data2,
                    "postgres", "1234"
            );

            if(connect2 == null){
                System.out.println("Not connected");
                System.exit(1);
            }

            ResultSet resultSet1;
            ResultSet resultSet2;
            field = fields.split(",");

            PreparedStatement prepare1 = connect1.prepareStatement(
                    "SELECT * FROM first_compr_db.public.addresses",
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);

            resultSet1 = prepare1.executeQuery();
            int sizeOfRes1 = getSize(resultSet1);

            List list1 = doList(resultSet1, sizeOfRes1);

            PreparedStatement prepare2 = connect1.prepareStatement(
                    "SELECT * FROM second_compr_db.public.addresses",
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);

            resultSet2 = prepare1.executeQuery();
            int sizeOfRes2 = getSize(resultSet2);

            List list2 = doList(resultSet2, sizeOfRes2);

            if (sizeOfRes1 != sizeOfRes2)
                return false;
            AtomicBoolean flag = new AtomicBoolean(true);
            list1.forEach(c -> {
                if(!list2.remove(c))
                    flag.set(false);
            });
            return flag.get();
        }
        catch (SQLException|ClassNotFoundException exc){
            exc.printStackTrace();
        }
        catch (Exception exc){
            exc.printStackTrace();
        }
        finally {
            if(connect1 != null){
                connect1.close();
            }
            if(connect2 != null){
                connect2.close();
            }
            //return false;
        }
        return false;
    }
}
