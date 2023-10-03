# Modifying the consumer script to handle tiered alerts for student logins


import pika
from datetime import datetime, timedelta

# Connection and queue declaration for RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='02-Login', durable=True)

def student_login_callback(ch, method, properties, body):
    # Decode the message and split into parts (student_id, last_login, total_logins)
    parts = body.decode().split(',')
    
    if len(parts) != 4:
        print("Received an invalid message format. Skipping.")
        return
    
    student_id, name, last_login, total_logins = parts
    last_login_date = datetime.strptime(last_login, '%Y-%m-%d %H:%M:%S')
    days_since_last_login = (datetime.now() - last_login_date).days
    
    # Tiered alert system
    if days_since_last_login >= 10:
        print(f"Tier 3 Alert: Student {student_id},{name} hasn't logged in for 10 or more days. Alerting student, teacher, and admin.")
    elif days_since_last_login >= 7:
        print(f"Tier 2 Alert: Student {student_id},{name} hasn't logged in for 7 or more days. Alerting student and teacher.")
    elif days_since_last_login >= 5:
        print(f"Tier 1 Alert: Student {student_id},{name} hasn't logged in for 5 days. Alerting teacher.")

channel.basic_consume(queue='02-Login', on_message_callback=student_login_callback, auto_ack=True)
print(' [*] Waiting for student login messages. To exit press CTRL+C')
channel.start_consuming()

