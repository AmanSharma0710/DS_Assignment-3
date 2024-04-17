all:
	docker build ./server -t serverim
	docker compose up

clean:
	docker stop loadbalancer && docker rm loadbalancer
	docker stop shardmanager && docker rm shardmanager
	docker ps -a --filter ancestor=serverim --format="{{.ID}}" | xargs docker stop | xargs docker rm
	docker compose down
	docker rmi shardmanagerim
	docker rmi loadbalancerim
	docker rmi serverim



