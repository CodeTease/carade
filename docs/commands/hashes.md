# Hashes

Hashes are maps between string fields and string values, so they are the perfect data type to represent objects (e.g., A User with a number of fields like name, surname, age, and so forth).

## Supported Commands

Carade supports standard Redis Hash commands.

*   **HSET key field value**: Set the string value of a hash field.
*   **HGET key field**: Get the value of a hash field.
*   **HMSET key field value ...**: Set multiple hash fields to multiple values.
*   **HGETALL key**: Get all the fields and values in a hash.
*   **HDEL key field**: Delete one or more hash fields.
*   **HINCRBY key field increment**: Increment the integer value of a hash field by the given number.

## Examples: User Profiles

Hashes are perfect for storing object-like data such as user profiles. Here is a practical example demonstrating how to use Carade's Hash commands in Java using the popular `Jedis` client.

```java
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import java.util.HashMap;
import java.util.Map;

public class UserProfileExample {
    public static void main(String[] args) {
        // Initialize Jedis Pool (connecting to Carade's default port with password)
        try (JedisPool pool = new JedisPool("localhost", 63790)) {
            
            try (Jedis jedis = pool.getResource()) {
                jedis.auth("teasertopsecret");
                String userId = "user:1001";

                // 1. Using HMSET to store multiple fields at once (name, email, age)
                Map<String, String> userProfile = new HashMap<>();
                userProfile.put("name", "Jane Doe");
                userProfile.put("email", "jane.doe@example.com");
                userProfile.put("age", "28");
                
                jedis.hmset(userId, userProfile);
                System.out.println("User profile saved.");

                // 2. Using HSET to update a single field
                jedis.hset(userId, "status", "active");

                // 3. Using HGET to retrieve a specific field
                String email = jedis.hget(userId, "email");
                System.out.println("Retrieved Email: " + email);

                // 4. Using HGETALL to retrieve the entire profile
                Map<String, String> retrievedProfile = jedis.hgetall(userId);
                System.out.println("Full Profile:");
                for (Map.Entry<String, String> entry : retrievedProfile.entrySet()) {
                    System.out.println("  " + entry.getKey() + ": " + entry.getValue());
                }

            } catch (JedisException e) {
                System.err.println("Carade connection or command error: " + e.getMessage());
            }
        }
    }
}
```

See the full list in [Compatibility Matrix](compatibility.md).
