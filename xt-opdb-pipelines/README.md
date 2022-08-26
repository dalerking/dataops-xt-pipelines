### Manual lambda testing
Install docker and docker-compose. It allows easy interaction with docker image there is a simple docker-compose added.

```
docker-compose up
```

Project directory is mounted as volume to allow code changing without need to rebuild docker image. If you need to attach profiler like py-spy you might want to change entrypoint in docker-compose. The default one in Dockerfile will be replaced allowing you to enter shell.


Rebuild of docker image is needed if you need to reinstall python dependencies. Just type:
```
docker-compose build
```


### To run lambda and test locally:
- paste into `.env` file credentails for qa / prod db:
  ```
  DB_DRIVER=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so
  DB_HOSTNAME=10.10.93.192
  DB_PORT=1433
  DB_DEFAULT_DATABASE=PSA
  DB_DOMAIN=USXPRESS.COM
  DB_USERNAME=SVC_VariantAWSUSXSQL
  DB_PASSWORD=
  ```
- open vpn connection for usx db
- run `docker-compose build` if container was not created previously
- run `docker-compose up function` - you should see that terminal is waiting for next steps
- in Postman send POST request `http://localhost:9000/2015-03-31/functions/function/invocations`
with event json in body
- in case if you still have a problem with sending request, check all running containers
  (if there is no job-runner already running)
