LOCAL_NAME    = s4-container
IMAGE_NAME    = docker.io/petersbattaglia/s4

build :
	sudo docker build --tag ${IMAGE_NAME} .

publish :
	sudo docker push ${IMAGE_NAME}

stop :
	sudo docker stop ${LOCAL_NAME}

remove :
	sudo docker rm -f ${LOCAL_NAME}

start :
	sudo docker start ${LOCAL_NAME}

run :
	@sudo docker run -p 5088:5000 \
		-v /etc/s4:/data \
		-e LOG_LEVEL='DEBUG' \
		--detach --name ${LOCAL_NAME} ${IMAGE_NAME}

test :
	curl -X GET -H "Content-Type: application/json" http://127.0.0.1:5088/healthcheck/deep | jq

logs :
	sudo docker logs -f --tail 50 ${LOCAL_NAME}

debug_container :
	sudo docker exec -it ${LOCAL_NAME} bash
