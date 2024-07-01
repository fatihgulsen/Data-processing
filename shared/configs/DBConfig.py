class PostgresConfig:
    HOST = "postgres"
    PORT = "5432"
    USER = "airflow"
    PASSWORD = "airflow"
    JDBC_URL = f"jdbc:postgresql://{HOST}:{PORT}/%s"


class S3Config:
    HOST = "minio"
    PORT = "9000"
    USER = "minio"
    PASSWORD = "miniosecret"
