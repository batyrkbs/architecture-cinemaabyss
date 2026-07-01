$ErrorActionPreference = "Stop"

docker build -t monolith:latest ./src/monolith
docker build -t movies-service:latest ./src/microservices/movies
docker build -t proxy-service:latest ./src/microservices/proxy
docker build -t events-service:latest ./src/microservices/events
