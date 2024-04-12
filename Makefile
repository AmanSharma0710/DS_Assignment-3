all:
	# docker build ./server -t serverim
	docker compose up

clean:
	docker stop loadbalancer && docker rm loadbalancer
	docker ps -a --filter ancestor=serverim --format="{{.ID}}" | xargs docker stop | xargs docker rm
	docker compose down
	docker rmi loadbalancerim
	# docker rmi serverim



