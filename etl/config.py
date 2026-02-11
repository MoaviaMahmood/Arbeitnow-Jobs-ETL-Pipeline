import os
from dataclasses import dataclass

@dataclass
class PostgresConfig:
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = 5432
    dbname: str = "jobs_db"
    user: str = "jobs_user"
    password: str = ""  # Empty password for trust authentication
    
    def get_connection_params(self):
        params = {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
        }
        # Only add password if it's not empty
        if self.password:
            params["password"] = self.password
        return params