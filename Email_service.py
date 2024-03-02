import imaplib
import email
import time
import os
import re
from config import *
from main import *
# from speed_load import *
from time import sleep
from json import dumps
from kafka import KafkaProducer, KafkaConsumer
from confluent_kafka import Consumer,Producer, KafkaError
import sys


# def send_msgs_to_producer(message):
#     producer_config = {
#         'bootstrap.servers': '192.168.29.128:9092',
#         'security.protocol': 'PLAINTEXT',  # Use PLAINTEXT for non-SSL connections
#     }

#     # Create a Kafka producer instance
#     producer = Producer(producer_config)

#     # Callback function for delivery reports
#     def delivery_report(err, message):
#         if err is not None:
#             print('Message delivery failed: {}'.format(err))
#         else:
#             print('Message delivered to {} '.format(message.topic()))


    
#     producer.produce('atm-proj', key=None, value=message, callback=delivery_report)

#     # Wait for any outstanding messages to be delivered and delivery reports to be received
#     producer.flush()


# def receive_msgs_to_consumer(stored_subj, filenames):
#     # Kafka broker address (replace with your server's address)
#     bootstrap_servers = '192.168.29.128:9092'

#     # Create Kafka consumer configuration
#     consumer_config = {
#         'bootstrap.servers': bootstrap_servers,
#         'security.protocol': 'PLAINTEXT',
#         'group.id': 'test-group',  # Consumer group ID
#         'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
#     }

#     # Create a Kafka consumer instance
#     consumer = Consumer(consumer_config)

#     # Subscribe to the Kafka topic
#     consumer.subscribe(['atm-proj'])

#     try:
#         message_count = 0
        
#         while message_count < 10:
#             msg = consumer.poll(1.0)  # Poll for new messages

#             if msg is None:
#                 continue

#             if not msg.error():
#                 print("Received message:", msg.value().decode('utf-8'))
#                 load_data(stored_subj, filenames)
#                 message_count += 1
#             elif msg.error().code() != KafkaError._PARTITION_EOF:
#                 print("Error occurred:", msg.error())

#     except Exception as e:
#         print(e)
#     finally:
#         consumer.close()
################################################################################
    # try:
    #     message_count=0
    #     while message_count<10:
    #         msg = consumer.poll(1.0)  # Poll for new messages

    #         if msg is None:
    #             continue

    #         for topic_partition, message in msg.items():
    #             print("Received message:", message.value().decode('utf-8'))
    #             load_data(stored_subj, filenames)

    # except Exception as e:
    #     print(e)
    # finally:
    #     consumer.close()    

# def receive_msgs_to_consumer(stored_subj,filenames):


#     for stored_sub,file in zip(stored_subj,filenames):
    
#         # Kafka broker address (replace with your server's address)
#         bootstrap_servers = '192.168.29.128:9092'

#         # Create Kafka consumer configuration
#         consumer_config = {
#             'bootstrap.servers': bootstrap_servers,
#             'security.protocol': 'PLAINTEXT',
#             'group.id': 'test-group',  # Consumer group ID
#             'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
#         }

#         # Create a Kafka consumer instance
#         consumer = Consumer(consumer_config)

#         # Subscribe to the Kafka topic
#         consumer.subscribe(['atm-proj'])


#         try:            
#             msg = consumer.poll(1.0)  # Poll for new messages
            
#             if msg:
#                 print("Received message:", msg.value().decode('utf-8'))
#                 load_data(stored_sub,file)
#             else: 
#                 continue
#         except Exception as e:
#             print (e)
#         finally:
#             consumer.close()
                

def get_emails():
    # start = time.time()
    query = "SELECT Bank_Name FROM Bank"
    df = pd.read_sql(query, engine)
    # stored_subj = []
    # filenames = []
    # Connect to the IMAP server
    with imaplib.IMAP4_SSL(IMAP_SERVER) as imap:
        imap.login(EMAIL, PASSWORD)
        imap.select('INBOX')
        stored_subj = []
        filenames = []
        # Search for unread emails
        _, messages = imap.search(None, '(UNSEEN)')

        for message_id in messages[0].split():
            _, msg_data = imap.fetch(message_id, '(RFC822)')
            msg = email.message_from_bytes(msg_data[0][1])
            subject_name = msg['Subject']
            split_list = re.split(r'(:|/)', subject_name)
            
            if split_list[0] == 'ReplenishmentOrder' and split_list[-1] in df['Bank_Name'].values:
                stored_subj.append(split_list[-1])

                for part in msg.walk():
                    if part.get_content_disposition() is not None:
                        filename = part.get_filename()

                        if filename and ('xlsx' in filename or 'csv' in filename):
                            filenames.append(filename)
                            current_time = datetime.now().strftime('%H%M%S')
                            filename_with_time = f"{current_time}_{filename}"
                            file_path = os.path.join(SOURCE_PATH, filename_with_time)

                            with open(file_path, 'wb') as f:
                                f.write(part.get_payload(decode=True))
                            print("File has been saved:", filename_with_time)
                            # send_msgs_to_producer(f'{filename} xlsx file is available')
                            # send_msgs_to_producer(f'{filename} xlsx file is available')
                            # print('message has been sent to kafka')
                            # receive_msgs_to_consumer(stored_subj,filenames)
                        
            else:
                # Mark the email as unread if it doesn't match the desired condition
                imap.store(message_id, '-FLAGS', '\\Seen')
                break
    
    #receive_msgs_to_consumer(stored_subj,filenames)             

    load_data(stored_subj, filenames)

    # # Close the IMAP connection
    # imap.close()
    # imap.logout()

    # end = time.time()
    # total = end - start
    # print(total)
 

def main():
    
    # while True:
        start = time.time()
        get_emails()
        end = time.time()
        total = end - start
        print(total)
        # time.sleep(40)

main()