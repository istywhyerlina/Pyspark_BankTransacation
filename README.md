
docker compose build --no-cache
docker compose up -d

docker exec -it pyspark_container pip install sqlalchemy psycopg2-binary