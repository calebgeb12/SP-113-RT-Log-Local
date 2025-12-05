# SP-113-RT-Log-Local

HOW TO SETUP THE PROJECT:

Clone this repository

Make sure you open four seperate linux terminals

1) in .../kafka-setup run the following in this order

docker compose up -d
docker exec -it kafka bash
kafka-console-consumer --topic web-logs --from-beginning --bootstrap-server localhost:9092


2) in .../producer run docker run --rm --network kafka-setup_default producer-app
3) in .../spark-setup run the following...
   docker run -d --name spark-consumer --network kafka-setup_default -p 5050:5050 -p 4040:4040 spark-consumer

   if you get a container running error run the following: docker stop spark-consumer && docker rm spark-consumer


4) in .../gui run python3 gui_display.py


