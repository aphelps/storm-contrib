package storm.kafka;

/**
 * Something went wrong when communicating with Kafka
 */
public class KafkaCommunicationsException extends RuntimeException  {
    private int m_reason;
    public KafkaCommunicationsException(int reason) {
        m_reason = reason;
    }
    public int getReason() {
        return  m_reason;
    }
}
