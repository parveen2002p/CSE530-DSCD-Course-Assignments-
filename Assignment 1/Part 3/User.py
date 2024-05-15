import pika,sys,os
import json

def main():

    def callback(ch, method, properties, body):
        print(body.decode())
    
    def updateSubscription(username,sub_or_nosub,youtubername):
        # message=username+','+sub_or_nosub+','+youtubername
        data={"username":username,
              "Subscription":sub_or_nosub,
              "Youtubername":youtuber}
        
        channel.basic_publish(exchange='', routing_key='UserToServer', body=json.dumps(data))

        
    
    def receiveNotification(username):
        channel.basic_consume(queue=username, on_message_callback=callback, auto_ack=True)
        return
    ip_addr=sys.argv[1]

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=ip_addr,credentials=pika.PlainCredentials('admin','admin')))
    channel = connection.channel()


    username=sys.argv[2]
    channel.queue_declare(queue='UserToServer')
    channel.queue_declare(queue=username)
    # channel.basic_publish(exchange='', routing_key='UserToServer', body='Logged in')
    if(len(sys.argv)==5):
        sub_or_unsub=sys.argv[3]
        youtuber=sys.argv[4]
        updateSubscription(username,sub_or_unsub,youtuber)
    else:
        data={"username":username,
              "Subscription":" ",
              "Youtubername":" "}
        channel.basic_publish(exchange='', routing_key='UserToServer', body=json.dumps(data))
    receiveNotification(username)
    
    # channel.basic_consume(queue='ServerToUser', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()



if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)