target/logd-0.1.0-SNAPSHOT-standalone.jar: src/**/*.clj project.clj
	lein uberjar

run: target/logd-0.1.0-SNAPSHOT-standalone.jar docker-compose.yml Dockerfile
	docker-compose build
	docker-compose up
