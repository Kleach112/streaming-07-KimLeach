# Modify the consumer script to decode the 03-Activity queue messages and generate alerts based on activity

import pika
from datetime import datetime

# Connection and queue declaration for RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='03-Activity', durable=True)

def activity_alert_callback(ch, method, properties, body):
    # Decode the message and split into parts (student_id, name, assignments_submitted, last_activity)
    parts = body.decode().split(',')
    
    if len(parts) != 4:
        print("Received an invalid message format. Skipping.")
        return
    
    student_id, name, assignments_submitted, last_activity = parts
    assignments_submitted = int(assignments_submitted)
    last_activity_date = datetime.strptime(last_activity, '%Y-%m-%d %H:%M:%S')
    days_since_last_activity = (datetime.now() - last_activity_date).days
    
    # Check if an alert should be triggered based on assignments and activity
    if assignments_submitted < 20 and days_since_last_activity > 7:
        print(f"Alert: Student {student_id},{name} has submitted fewer than 20 assignments and it's been more than 7 days since their last activity.")

channel.basic_consume(queue='03-Activity', on_message_callback=activity_alert_callback, auto_ack=True)
print(' [*] Waiting for student activity messages. To exit press CTRL+C')
channel.start_consuming()

