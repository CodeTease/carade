# Strings

String keys are binary safe, meaning they can contain any kind of data, for instance a JPEG image or a serialized Ruby object.
A string value can be at most 512 Megabytes.

## Supported Commands

Carade supports standard Redis String commands.

*   **SET key value**: Set the string value of a key.
*   **GET key**: Get the value of a key.
*   **INCR key**: Increment the integer value of a key by one.
*   **DECR key**: Decrement the integer value of a key by one.
*   **APPEND key value**: Append a value to a key.
*   **STRLEN key**: Get the length of the value stored in a key.
*   **SETEX key seconds value**: Set the value and expiration of a key.

## Examples: Session Management

Using `SETEX` is ideal for implementing session management systems, combining the atomic operation of setting a value and an expiration time.

### HTTP API Example

HTTP Request:
```http
POST /command HTTP/1.1
Host: localhost:63790
Content-Type: text/plain

SETEX session:user123 3600 "{\"userId\": 123, \"role\": \"admin\"}"
```

HTTP Response:
```http
HTTP/1.1 200 OK
Content-Type: text/plain

OK
```

### Python (redis-py) Example

```python
import redis
import json

r = redis.Redis(host='localhost', port=63790, password='teasertopsecret')

def create_session(user_id, role, session_timeout_seconds=3600):
    session_key = f"session:{user_id}"
    session_data = json.dumps({"userId": user_id, "role": role})

    # Store the session data and set it to expire automatically
    r.setex(session_key, session_timeout_seconds, session_data)
    print(f"Session created for user {user_id}. Expires in {session_timeout_seconds}s.")

def get_session(user_id):
    session_key = f"session:{user_id}"
    session_data = r.get(session_key)

    if session_data:
        return json.loads(session_data)
    return None # Session expired or doesn't exist

# Usage
create_session("user123", "admin")
# ... later ...
session = get_session("user123")
if session:
    print(f"Logged in as {session['role']}")
else:
    print("Session expired, please login again.")
```

See the full list in [Compatibility Matrix](compatibility.md).
