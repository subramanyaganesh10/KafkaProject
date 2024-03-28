from confluent_kafka import Consumer, KafkaError
import matplotlib.pyplot as plt
from elasticsearch import Elasticsearch, helpers
import json

es = Elasticsearch('http://localhost:9200',
                   basic_auth=("elastic", "elasticsearch"))

def consume_messages():
    conf = {
        'bootstrap.servers': 'localhost:9092',  # Change this to your Kafka broker's address
        'group.id': 'group23',        # Consumer group ID
        'auto.offset.reset': 'earliest'         # Start consuming from the earliest available message
    }

    consumer = Consumer(conf)

    # Subscribe to the Kafka topic
    consumer.subscribe(['netflix'])  

    # Set up the initial plot
    x_values = []
    y_values = []
    plt.ion()  # Turn on interactive mode
    fig, ax = plt.subplots()
    line, = ax.plot(x_values, y_values)
    ax.set_xlabel('Message Count')
    ax.set_ylabel('Float Value')
    ax.set_title('The stock price of Netflix from Kafka')
    ax.grid(True)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Poll for messages
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition, consumer reached end of Kafka topic
                    continue
                else:
                    # Some other error
                    print(msg.error())
                    break

            # Process the received message
            value = msg.value().decode('utf-8')  # Decode the message value
            json_value = json.loads(value)
            value = json_value['values']
            print("Received message:",value)
            es.index(index='netflix',document={'value':value,'timestamp':json_value['date']})
            # Convert value to float (assuming it's a float value)
            float_value = float(value)

            # Append index (message count) and value to the lists
            x_values.append(len(y_values))
            y_values.append(float_value)

            # Update the plot with new data
            line.set_xdata(x_values)
            line.set_ydata(y_values)
            ax.relim()
            ax.autoscale_view()
            fig.canvas.draw()
            fig.canvas.flush_events()

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()  # Close the consumer when done

if __name__ == "__main__":
    consume_messages()
