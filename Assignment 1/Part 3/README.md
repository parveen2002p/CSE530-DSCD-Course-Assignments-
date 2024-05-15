# DSCD Assignment Youtube Server
- ## YoutubeServer.py
    To run the ```YoutuberServer.py``` run the command
    ```bash
    python YoutuberServer.py
- ## Youtuber.py
    ```Youtuber.py``` allows youtuber to publish video to the YoutubeServer. To publish the video run the command:

    python &lt;Youtuber&gt; &lt;Videoname&gt; &lt;IP address of YoutubeServer&gt;

    ```bash
    python Youtuber.py Aakash First Video 10.190.10.2
    ```
    Here the Youtuber name is Aakash and the Videoname is First Video

- ## User.py
    ```User.py``` allow user to subscribe/unsubscribe the available youtubers.

    User have three options
    * Subscribe to the Youtuber
    * UnSubscribe to the Youtuber
    * Login and don't subscribe/unsubscribe
    
    ### For Subscribing to the Youtuber
    ```bash
    python 10.190.10.2 User.py Shubham s Aakash
    ```
    Shubham subscribes to Aakash
    ### For Unsubscribing to the Youtuber
    ```bash
    python 10.190.10.2 User.py Parveen u Aakash
    ```
    Parveen subscribes to Aakash
    ### For Login only
    ```bash
    python 10.190.10.2 User.py Shubham
    ```
    Shubham logged in
 
    
