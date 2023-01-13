import os
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from models import DeclarativeBase
engine = sa.create_engine(
    "sqlite:///" + os.path.join(os.path.dirname(__file__), 'scheduler.db'), echo=True)
SessionFactory = sessionmaker(bind=engine, autoflush=True)
DeclarativeBase.metadata.create_all(engine)
