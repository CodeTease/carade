package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class LcsCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: LCS key1 key2 [LEN]");
            return;
        }

        String key1 = new String(args.get(1), StandardCharsets.UTF_8);
        String key2 = new String(args.get(2), StandardCharsets.UTF_8);
        boolean lenOnly = false;
        
        if (args.size() > 3) {
            String opt = new String(args.get(3), StandardCharsets.UTF_8).toUpperCase();
            if (opt.equals("LEN")) {
                lenOnly = true;
            } 
            // IDX, MINMATCHLEN etc not implemented yet
        }
        
        ValueEntry v1 = Carade.db.get(client.dbIndex, key1);
        ValueEntry v2 = Carade.db.get(client.dbIndex, key2);
        
        if ((v1 != null && v1.type != DataType.STRING) || (v2 != null && v2.type != DataType.STRING)) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }
        
        String s1 = (v1 == null) ? "" : new String((byte[])v1.getValue(), StandardCharsets.UTF_8);
        String s2 = (v2 == null) ? "" : new String((byte[])v2.getValue(), StandardCharsets.UTF_8);
        
        if (lenOnly) {
            int l = computeLcsLen(s1, s2);
            client.sendInteger(l);
        } else {
            String lcs = computeLcsString(s1, s2);
            client.sendBulkString(lcs);
        }
    }
    
    private int computeLcsLen(String s1, String s2) {
        int m = s1.length();
        int n = s2.length();
        int[][] dp = new int[2][n + 1]; // Optimize space to O(min(m,n)) rows? Or just 2 rows.

        for (int i = 1; i <= m; i++) {
            for (int j = 1; j <= n; j++) {
                if (s1.charAt(i - 1) == s2.charAt(j - 1)) {
                    dp[i % 2][j] = dp[(i - 1) % 2][j - 1] + 1;
                } else {
                    dp[i % 2][j] = Math.max(dp[(i - 1) % 2][j], dp[i % 2][j - 1]);
                }
            }
        }
        return dp[m % 2][n];
    }

    private String computeLcsString(String s1, String s2) {
        int m = s1.length();
        int n = s2.length();
        int[][] dp = new int[m + 1][n + 1];

        for (int i = 1; i <= m; i++) {
            for (int j = 1; j <= n; j++) {
                if (s1.charAt(i - 1) == s2.charAt(j - 1)) {
                    dp[i][j] = dp[i - 1][j - 1] + 1;
                } else {
                    dp[i][j] = Math.max(dp[i - 1][j], dp[i][j - 1]);
                }
            }
        }

        int index = dp[m][n];
        char[] lcs = new char[index];
        int i = m, j = n;
        while (i > 0 && j > 0) {
            if (s1.charAt(i - 1) == s2.charAt(j - 1)) {
                lcs[index - 1] = s1.charAt(i - 1);
                i--;
                j--;
                index--;
            } else if (dp[i - 1][j] > dp[i][j - 1]) {
                i--;
            } else {
                j--;
            }
        }
        return new String(lcs);
    }
}
