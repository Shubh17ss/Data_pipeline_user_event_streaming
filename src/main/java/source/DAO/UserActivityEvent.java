package source.DAO;

 public class UserActivityEvent {
        public String userId;
        public String eventId;
        public String eventType;
        public String pageId;
        public long timestamp;

        // No-arg constructor for Jackson deserialization
        public UserActivityEvent() {
        }

        public UserActivityEvent(String userId, String eventId, String eventType, String pageId, long timestamp) {
            this.userId = userId;
            this.eventId = eventId;
            this.eventType = eventType;
            this.pageId = pageId;
            this.timestamp = timestamp;
        }

        public String getUserId() {
            return userId;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
