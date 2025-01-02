from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class RawLog(Base):
    __tablename__ = 'raw_logs'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    content = Column(String)

class Anomaly(Base):
    __tablename__ = 'anomalies'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    text = Column(String)
    score = Column(Float)

# Initialize database
engine = create_engine('sqlite:///logs.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine) 