FROM kafka-microservice

ADD demo_service.py $APP_DIR

CMD [ "uwsgi", "--ini", "uwsgi.ini", "--mule=demo_service.py", "--mule=demo_service.py" ]
