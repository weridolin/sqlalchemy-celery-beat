import os
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
import conf

from models import DeclarativeBase
engine = sa.create_engine(
    conf.SQLALCHEMY_URL, 
    echo=False)
SessionFactory = sessionmaker(bind=engine, autoflush=True)
DeclarativeBase.metadata.create_all(engine)
