#include <stdio.h>
#include <string.h>
#include <librdkafka/rdkafka.h>

int main(int argc, char **argv) {
    rd_kafka_t *rk;             /* Kafka producer instance handle */
    rd_kafka_conf_t *conf;      /* Temporary configuration object */
    char errstr[512];           /* librdkafka API error reporting buffer */
    const char *brokers;        /* Kafka broker(s) */
    const char *topic;          /* Kafka topic to produce to */
    int partition = RD_KAFKA_PARTITION_UA; /* Kafka partition to produce to */
    int max_msg_size = 100000;  /* Maximum size of Kafka messages */
    char *output_file = "kafka_data.txt"; /* Output file name */

    /* Check arguments */
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <brokers> <topic>\n", argv[0]);
        return 1;
    }
    brokers = argv[1];
    topic = argv[2];

    /* Set up Kafka configuration */
    conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error setting broker list: %s\n", errstr);
        return 1;
    }

    /* Create Kafka producer instance */
    rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        fprintf(stderr, "Error creating Kafka producer: %s\n", errstr);
        return 1;
    }

    /* Subscribe to Kafka topic */
    if (rd_kafka_subscribe(rk, topic) != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "Error subscribing to Kafka topic: %s\n", rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_destroy(rk);
        return 1;
    }

    /* Open output file for writing */
    FILE *fp = fopen(output_file, "w");
    if (!fp) {
        fprintf(stderr, "Error opening output file: %s\n", strerror(errno));
        rd_kafka_destroy(rk);
        return 1;
    }

    /* Continuously poll for new Kafka messages */
    while (1) {
        rd_kafka_message_t *msg;

        /* Wait for Kafka message or error */
        msg = rd_kafka_consumer_poll(rk, 1000);
        if (!msg) {
            continue;
        }

        /* Check for Kafka message error */
        if (msg->err) {
            if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                fprintf(stderr, "Reached end of partition: %s [%d] at offset %ld\n",
                        rd_kafka_topic_name(msg->rkt), msg->partition, msg->offset);
            } else {
                fprintf(stderr, "Error receiving message: %s\n", rd_kafka_message_errstr(msg));
            }
            rd_kafka_message_destroy(msg);
            continue;
        }

        /* Write message contents to output */
        fwrite(msg->payload, 1, msg->len, fp);
        fprintf(stdout, "Received message (%zd bytes) on topic %s partition %d\n", msg->len, rd_kafka_topic_name(msg->rkt), msg->partition);
        rd_kafka_message_destroy(msg);
    }
    /* Close output file */
fclose(fp);

/* Destroy Kafka producer instance */
rd_kafka_destroy(rk);

return 0;
}
