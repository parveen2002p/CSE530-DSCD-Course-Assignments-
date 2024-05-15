#!/usr/bin/env python
import pika, sys, os,json

def main():
    youtubers=set()
    youtubers_subscriber=dict()
    youtubers_videos=dict()
    users=set()
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='0.0.0.0',credentials=pika.PlainCredentials('admin','admin')))
    channel = connection.channel()

    channel.queue_declare(queue='YoutuberToServer')
    # channel.queue_declare(queue='ServerToYoutuber')
    channel.queue_declare(queue='UserToServer')
   

    def consume_user_requests(ch, method, properties, body):
        # message=body.decode().split(',')
        message=json.loads(body.decode())
        username=message["username"]
        if(len(message["Subscription"])!=0):

            sub_or_nosub=message["Subscription"]
            youtuber=message["Youtubername"]
        else:
            sub_or_nosub=" "
            youtuber=" "
        print(username+" Logged in")
        if(username not in users):
            users.add(username)
        
        channel.queue_declare(queue=username)

        if(youtuber not in youtubers_subscriber and youtuber!=" "):
            channel.basic_publish(exchange='', routing_key=username, body='No Youtuber Found')
        elif(youtuber==" "):
            channel.basic_publish(exchange='', routing_key=username, body='Loggedin Successfully')
        else:
            if(sub_or_nosub=='s'):
                if(username in youtubers_subscriber[youtuber]):
                    mess="Already Subscribed"
                    channel.basic_publish(exchange='', routing_key=username, body=mess)
                else:
                    youtubers_subscriber[youtuber].add(username)
                    print(username," Subscribed to ",youtuber)
                    channel.basic_publish(exchange='', routing_key=username, body='Updated Subscription')
            elif(sub_or_nosub=='u'):
                if(username not in youtubers_subscriber[youtuber]):
                    mess = "Already Unsubscribed"
                    channel.basic_publish(exchange='', routing_key=username, body=mess)
                else:
                    youtubers_subscriber[youtuber].remove(username)
                    print(username," unsubscribed to ",youtuber)
                    channel.basic_publish(exchange='', routing_key=username, body='Updated Subscription')
            else:
                mess = "Type s/u for subscribe/unsubscribe respectively"
                channel.basic_publish(exchange='', routing_key=username, body=mess)
            
        return

    def notify_users(youtubername,videoname):
        subscribed_users=youtubers_subscriber[youtubername]
        message="New Notification: "+youtubername+' uploaded '+videoname
        for i in subscribed_users:
            channel.basic_publish(exchange='', routing_key=i, body=message)
        
        return
    
    def consume_youtuber_requests(ch, method, properties, body):


        youtubername=body.decode().split('#')[0]
        videoname=body.decode().split('#')[1]
        # print("YoutuberName",youtubername)
        if(youtubername not in youtubers):
            youtubers.add(youtubername)
            youtubers_subscriber[youtubername]=set()
            youtubers_videos[youtubername]=list()
            
        youtubers_videos[youtubername].append(videoname)
        channel.queue_declare(queue=youtubername)
        print(youtubername," uploaded ",videoname)
        channel.basic_publish(exchange='', routing_key=youtubername, body=videoname+' Uploaded Succesfully')
        notify_users(youtubername,videoname)
        return
           
        
    

    t1=channel.basic_consume(queue='YoutuberToServer', on_message_callback=consume_youtuber_requests, auto_ack=True)
    channel.basic_consume(queue='UserToServer', on_message_callback=consume_user_requests, auto_ack=True)
    # print("T1",t1)
    print(' [*] Waiting for messages. To exit press CTRL+C')
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