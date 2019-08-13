package fi.hsl.transitdata.railsource;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.TransitdataProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
class RailAlertService {
    private Producer<byte[]> producer;

    RailAlertService(Producer<byte[]> producer) {
        this.producer = producer;
    }

    void handleRailAlerts(GtfsRealtime.FeedMessage feedMessage) {
        List<GtfsRealtime.Alert> alerts = filterAlerts(feedMessage);
        log.info("Found {} rail alerts", alerts.size());
        final long timestampMs = feedMessage.getHeader().getTimestamp() * 1000;
        alerts
                .forEach(alert -> {
                    try {
                        handleAlert(alert, timestampMs);
                    } catch (PulsarClientException e) {
                        e.printStackTrace();
                    }
                });
    }

    private void handleAlert(GtfsRealtime.Alert alert, long timestampMs) throws PulsarClientException {
        try {
            producer.newMessage().value(alert.toByteArray())
                    .eventTime(timestampMs)
                    .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.TransitdataServiceAlert.toString())
                    .send();
        } catch (PulsarClientException pe) {
            log.error("Failed to send message to Pulsar", pe);
            throw pe;
        } catch (Exception e) {
            log.error("Failed to handle alert message", e);
        }
    }

    private List<GtfsRealtime.Alert> filterAlerts(GtfsRealtime.FeedMessage feedMessage) {
        return feedMessage.getEntityList()
                .stream()
                .filter(GtfsRealtime.FeedEntity::hasAlert)
                .map(GtfsRealtime.FeedEntity::getAlert)
                .collect(Collectors.toList());
    }
}
