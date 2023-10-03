# Modified emitter script to stream student data to RabbitMQ


# Import necessary libraries
from datetime import datetime, timedelta
import pika
import sys
import webbrowser
import time
from faker import Faker
import random
from inspect import getsource

fake = Faker()

def corrected_generate_student_data(num_students=100):
    students = []
    for i in range(num_students):
        student = {
            'student_id': fake.uuid4(),
            'name': fake.name(),
            'email': fake.email(),
            'enrollment_date': fake.date_this_decade(before_today=True, after_today=False),
            'total_logins': random.randint(1, 50)
        }
        
        # Ensure some students meet the login alert criteria
        if i < num_students * 0.1:  # 10% of students
            delta = timedelta(days=5)
        elif i < num_students * 0.2:  # next 10% of students
            delta = timedelta(days=7)
        elif i < num_students * 0.3:  # next 10% of students
            delta = timedelta(days=10)
        else:
            delta = timedelta(days=random.randint(0, 4))  # remaining 70% of students
        
        student['last_login'] = (datetime.now() - delta).strftime('%Y-%m-%d %H:%M:%S')
        
        # Ensure some students meet the no alert criteria (Criteria 1 for activity)
        if i < num_students * 0.4:  # 40% of students
            student['assignments_submitted'] = 20
            student['last_activity'] = fake.date_time_this_month(before_now=True, after_now=False, tzinfo=None)
        elif i < num_students * 0.8:  # next 40% of students
            student['assignments_submitted'] = random.randint(0, 19)
            
            # Generate a random datetime within the last week
            delta = timedelta(days=random.randint(0, 7))
            student['last_activity'] = (datetime.now() - delta).strftime('%Y-%m-%d %H:%M:%S')
        # Ensure some students trigger the alert (Criteria 2 for activity)
        else:  # remaining 20% of students
            student['assignments_submitted'] = random.randint(0, 19)
            delta = timedelta(days=random.randint(8, 30))
            student['last_activity'] = (datetime.now() - delta).strftime('%Y-%m-%d %H:%M:%S')
        
        students.append(student)
    
    return students


def offer_rabbitmq_admin_site():
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def send_message(host: str, queue_name: str, message: str):
    if not message.strip():
        return

    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        ch = conn.channel()
        ch.queue_declare(queue=queue_name, durable=True)
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        print(f" [x] Sent {message} to {queue_name} queue")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    offer_rabbitmq_admin_site()
    
    students = corrected_generate_student_data()

    for student in students:
        # Construct messages for each queue
        message_enrollment = f"{student['student_id']},{student['enrollment_date']}"
        message_login = f"{student['student_id']},{student['name']},{student['last_login']},{student['total_logins']}"
        message_activity = f"{student['student_id']},{student['name']},{student['assignments_submitted']},{student['last_activity']}"
        
        # Send messages to respective queues
        send_message("localhost", "01-Enrollment", message_enrollment)
        send_message("localhost", "02-Login", message_login)
        send_message("localhost", "03-Activity", message_activity)
        
        # Delay between each student data to simulate streaming
        time.sleep(random.randint(2, 10))
    
    print("All student data have been sent to the RabbitMQ queues!")

