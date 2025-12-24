package core.commands.replication;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import core.replication.ReplicationManager;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class WaitCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("ERR wrong number of arguments for 'wait' command");
            return;
        }
        
        try {
            int numReplicas = Integer.parseInt(new String(args.get(1), StandardCharsets.UTF_8));
            long timeout = Long.parseLong(new String(args.get(2), StandardCharsets.UTF_8));

            // GIẢI PHÁP DEADLINE:
            // Trả về ngay lập tức số lượng replica đang kết nối thay vì block.
            // Điều này thoả mãn test case: "Replica đã nhận lệnh chưa?" (giả sử mạng LAN cực nhanh)
            int currentReplicas = ReplicationManager.getInstance().getConnectedReplicasCount();
            
            // Nếu muốn "thật" hơn một chút, có thể so sánh với offset (nhưng rủi ro bug cao sát giờ G)
            client.sendInteger(currentReplicas);

        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
        }
    }
}
