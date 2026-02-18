# Geospatial

Carade supports storing and querying geospatial data using the GeoHash algorithm. It allows you to store coordinates (longitude, latitude) and query for items within a given radius.

## Supported Commands

Carade supports standard Redis Geo commands.

*   **GEOADD key longitude latitude member**: Add one or more geospatial items to the specified key.
*   **GEODIST key member1 member2 [unit]**: Return the distance between two members in the geospatial index.
*   **GEORADIUS key longitude latitude radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key]**: Query a sorted set representing a geospatial index to fetch members matching a given maximum distance from a point.
*   **GEORADIUSBYMEMBER key member radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key]**: Query a sorted set representing a geospatial index to fetch members matching a given maximum distance from a member.
*   **GEOHASH key member**: Return valid Geohash strings representing the position of one or more elements in a sorted set value representing a geospatial index.
*   **GEOPOS key member**: Return the positions (longitude,latitude) of all the specified members of the geospatial index represented by the sorted set at key.

See the full list in [Compatibility Matrix](compatibility.md).
