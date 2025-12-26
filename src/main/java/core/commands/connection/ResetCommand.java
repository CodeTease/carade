package core.commands.connection;

import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class ResetCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // RESET
        // Returns +RESET
        
        // 1. Reset DB
        // client.dbIndex = 0; // Access is package-private or public field?
        // ClientHandler.dbIndex is public field.
        client.dbIndex = 0;
        
        // 2. Unwatch
        client.unwatchAll();
        
        // 3. Discard transaction
        if (client.isInTransaction()) {
            client.setInTransaction(false);
            client.clearTransactionQueue();
            client.setTransactionDirty(false);
            client.setCaptureBuffer(null);
        }
        
        // 4. Reset User (Auth) - Redis 6.2+ behavior: RESET returns to "default" user (unauthenticated state or default user state)
        // Actually RESET usually resets connection state. 
        // "This command performs the following operations: ... Discards the current transaction ... Unwatches ... Resets the authenticated user to the default user ..."
        client.setCurrentUser(null); // Or default? 
        // If system has "default" user with no pass, we should set it?
        // Logic in ClientHandler: if currentUser == null and no AUTH sent, it denies.
        // But initially connections are unauthenticated unless "default" user has no password.
        // In `Carade.java`, we see:
        // config.users.put("default", new User("default", config.password, true, true));
        // If config.password is set, default user has password.
        // If config.password is null (or empty?), default user has it too.
        // We should set currentUser to null to force re-auth if password exists.
        
        // 5. Client Name
        client.setClientName(null);
        
        // 6. Monitor/Sub? 
        // "RESET also behaves like QUIT if the client is in Pub/Sub or Monitor mode."
        // We need to check those.
        if (client.isMonitor()) {
            client.setMonitor(false);
            core.Carade.monitors.remove(client);
        }
        if (client.isSubscribed) {
            core.Carade.pubSub.unsubscribeAll(client);
        }
        
        client.sendSimpleString("RESET");
    }
}
