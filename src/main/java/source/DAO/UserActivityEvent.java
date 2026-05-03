package source.DAO;

 public class UserActivityEvent {
        public String eventId;
        public String eventType;
        public String pageId;
        public long timestamp;

        // No-arg constructor for Jackson deserialization
        public UserActivityEvent() {
        }

        public UserActivityEvent(String eventId, String eventType, String pageId, long timestamp) {
            this.eventId=eventId;
            this.eventType = eventType;
            this.pageId = pageId;
            this.timestamp = timestamp;
        }
    }
