package fi.hsl.transitdata.railsource;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.TransitdataProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static fi.hsl.transitdata.railsource.RailSpecific.filterAlerts;

/**
 * Sends parsed railway alerts to a Pulsar topic
 */
@Slf4j
class RailAlertService {
    private Producer<byte[]> producer;

    RailAlertService(Producer<byte[]> producer) {
        this.producer = producer;
    }

    Integer sendRailAlerts(GtfsRealtime.FeedMessage feedMessage) {
        AtomicReference<Integer> sentRailAlerts = new AtomicReference<>(0);
        List<GtfsRealtime.Alert> alerts = filterAlerts(feedMessage);
        log.info("Found {} rail alerts", alerts.size());
        final long timestampMs = feedMessage.getHeader().getTimestamp() * 1000;
        sendFilteredAlerts(sentRailAlerts, alerts, timestampMs);
        return sentRailAlerts.get();
    }

    private void sendFilteredAlerts(AtomicReference<Integer> sentRailAlerts, List<GtfsRealtime.Alert> alerts, long timestampMs) {
        alerts
                .forEach(alert -> {
                    try {
                        sendRailAlerts(alert, timestampMs);
                        sentRailAlerts.getAndSet(sentRailAlerts.get() + 1);
                    } catch (PulsarClientException e) {
                        e.printStackTrace();
                    }
                });
    }

    private void sendRailAlerts(GtfsRealtime.Alert alert, long timestampMs) throws PulsarClientException {
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


}
