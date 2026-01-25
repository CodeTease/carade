package core.commands.geo;

import core.Carade;
import core.db.CaradeDatabase;
import core.network.ClientHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class GeoSpatialTest {

    static class MockClientHandler extends ClientHandler {
        public Long lastIntegerResponse;
        public String lastResponse;
        public List<String> lastArray = new ArrayList<>();

        @Override
        public void sendInteger(long value) {
            this.lastIntegerResponse = value;
        }

        @Override
        public void sendBulkString(String value) {
            this.lastResponse = value;
        }

        @Override
        public void sendArray(List<byte[]> list) {
            this.lastArray.clear();
            for (byte[] o : list) {
                this.lastArray.add(new String(o, StandardCharsets.UTF_8));
            }
        }

        @Override
        public void sendMixedArray(List<Object> list) {
            this.lastArray.clear();
            for (Object o : list) {
                if (o instanceof byte[]) {
                    this.lastArray.add(new String((byte[]) o, StandardCharsets.UTF_8));
                } else {
                    this.lastArray.add(o.toString());
                }
            }
        }
    }

    @BeforeEach
    public void setup() {
        Carade.db = CaradeDatabase.getInstance();
        CaradeDatabase.getInstance().clearAll();
    }

    private List<byte[]> makeArgs(String... args) {
        List<byte[]> list = new ArrayList<>();
        for (String s : args) {
            list.add(s.getBytes(StandardCharsets.UTF_8));
        }
        return list;
    }

    @Test
    public void testGeoAddAndDist() {
        GeoAddCommand geoAdd = new GeoAddCommand();
        GeoDistCommand geoDist = new GeoDistCommand();
        MockClientHandler client = new MockClientHandler();

        // GEOADD Sicily 13.361389 38.115556 "Palermo" 15.087269 37.502669 "Catania"
        geoAdd.execute(client, makeArgs("GEOADD", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"));
        assertEquals(2L, client.lastIntegerResponse);

        // GEODIST Sicily Palermo Catania m
        // Distance should be ~166km
        geoDist.execute(client, makeArgs("GEODIST", "Sicily", "Palermo", "Catania", "km"));
        assertNotNull(client.lastResponse);
        double dist = Double.parseDouble(client.lastResponse);
        assertTrue(dist > 166.0 && dist < 167.0, "Distance " + dist + " should be around 166.27 km");
    }

    @Test
    public void testGeoRadius() {
        GeoAddCommand geoAdd = new GeoAddCommand();
        GeoRadiusCommand geoRadius = new GeoRadiusCommand();
        MockClientHandler client = new MockClientHandler();

        // Add cities
        geoAdd.execute(client, makeArgs("GEOADD", "Sicily", 
            "13.361389", "38.115556", "Palermo", 
            "15.087269", "37.502669", "Catania"));

        // GEORADIUS Sicily 15 37 200 km
        // Should find Catania (closer to 15, 37) and Palermo (maybe further?)
        // Catania is at 15.08, 37.50. Distance from 15,37 is small (<100km).
        // Palermo is at 13.36, 38.11. Distance from 15,37 is > 100km?
        // Let's check logic:
        // Dist(15,37 -> 13.36, 38.11) ~ 180km.
        
        geoRadius.execute(client, makeArgs("GEORADIUS", "Sicily", "15", "37", "200", "km"));
        // Expect both
        assertTrue(client.lastArray.size() >= 2, "Should find at least 2 cities");
        assertTrue(client.lastArray.contains("Palermo"));
        assertTrue(client.lastArray.contains("Catania"));

        geoRadius.execute(client, makeArgs("GEORADIUS", "Sicily", "15", "37", "100", "km"));
        // Expect only Catania
        assertTrue(client.lastArray.contains("Catania"));
        assertFalse(client.lastArray.contains("Palermo"));
    }
}
