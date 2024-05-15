import sys,os
import pika
import time
    
    # connection.close()

def main():
    
    def publishVideo(name,videoname):

        fullmessage=name+'#'+videoname
        channel.basic_publish(exchange='', routing_key='YoutuberToServer', body=fullmessage)
        return

    ip_addr=sys.argv[len(sys.argv)-1]
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=ip_addr,credentials=pika.PlainCredentials('admin','admin')))
    channel = connection.channel()



    name=sys.argv[1]
    
    videoname=""
    for i in range(2,len(sys.argv)-1):
        videoname+=sys.argv[i]+" "
        
    channel.queue_declare(queue='YoutuberToServer')
    channel.queue_declare(queue=name)

    publishVideo(name,videoname)
    print("Video uploaded successfully")
    

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)