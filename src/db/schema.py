from sqlalchemy import (
    create_engine,
    Column, 
    Integer, 
    Float, 
    String, 
    Boolean, 
    Date, 
    DateTime, 
    UniqueConstraint,
)

from sqlalchemy.orm import declarative_base
from src.utils.config import DB_URL

