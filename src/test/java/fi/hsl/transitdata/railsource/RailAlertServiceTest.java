package fi.hsl.transitdata.railsource;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

@Slf4j
public class RailAlertServiceTest {

    private static GtfsRealtime.FeedMessage FEEDMESSAGE = null;
    private static PulsarApplicationContext context;
    private RailAlertService railAlertService;


    @Before
    public void init() {
        FEEDMESSAGE = TestUtils.readExample();
        Producer mock = mock(Producer.class);
        this.railAlertService = new RailAlertService(mock);
    }

    @Test
    public void handleRailAlerts_sendValidAlert_shouldSendToProducer() {
        this.railAlertService.handleRailAlerts(FEEDMESSAGE);
    }

}
