import com.datatrees.datacenter.table.api.RpcClient;
import com.datatrees.datacenter.table.api.SchemaRequest;
import com.datatrees.datacenter.table.api.SchemaResponse;

public class RpcTest {

    public static void main(String[] args) throws Exception {
        SchemaRequest request = new SchemaRequest("t_tel_base_info", "operator53", 3, 1523177395);
        SchemaResponse rsp = new RpcClient("localhost", 8801).querySchema(request, 100);
        System.out.println(rsp.toString());
    }
}
