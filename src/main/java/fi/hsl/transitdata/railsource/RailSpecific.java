package fi.hsl.transitdata.railsource;

import com.google.transit.realtime.GtfsRealtime;

import java.util.List;
import java.util.stream.Collectors;

class RailSpecific {
    static List<GtfsRealtime.Alert> filterAlerts(GtfsRealtime.FeedMessage feedMessage) {
        return feedMessage.getEntityList()
                .stream()
                .filter(GtfsRealtime.FeedEntity::hasAlert)
                .map(GtfsRealtime.FeedEntity::getAlert)
                .collect(Collectors.toList());
    }
}
