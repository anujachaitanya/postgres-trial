from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


class PostgresClient:
    def init(self, host, database, user, password, port=5432):
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(connection_string)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def fetch(self, query, params=None):
        result = self.session.execute(text(query), params or {})
        if result.returns_rows:
            return result.fetchall()
        return None

    def close(self):
        self.session.close()
        self.engine.dispose()
