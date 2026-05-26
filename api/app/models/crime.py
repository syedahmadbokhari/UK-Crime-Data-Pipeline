from sqlalchemy import Column, Integer, String, Float
from app.database import Base


class Crime(Base):
    __tablename__ = "crimes"

    id = Column(Integer, primary_key=True, index=True)
    month = Column(String, index=True, nullable=False)
    force = Column(String, index=True, nullable=False)
    crime_type = Column(String, index=True, nullable=False)
    lsoa_code = Column(String, nullable=True)
    lsoa_name = Column(String, nullable=True)
    outcome = Column(String, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
