import json
from datetime import datetime
from typing import Set, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends
from pydantic import BaseModel, field_validator

from sqlalchemy.sql import select, insert, update, delete
from sqlalchemy import (create_engine,
                        MetaData,
                        Table,
                        Column,
                        Integer,
                        String,
                        Float,
                        DateTime)
from sqlalchemy.orm import sessionmaker

from config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB


# SQLAlchemy setup
DATABASE_URL = (f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
                f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
engine = create_engine(DATABASE_URL)
metadata = MetaData()
# Define the ProcessedAgentData table
processed_agent_data = Table(
 "processed_agent_data",
 metadata,
 Column("id", Integer, primary_key=True, index=True),
 Column("road_state", String),
 Column("x", Float),
 Column("y", Float),
 Column("z", Float),
 Column("latitude", Float),
 Column("longitude", Float),
 Column("timestamp", DateTime),
)

SessionLocal = sessionmaker(bind=engine)

# FastAPI models
class AccelerometerData(BaseModel):
 x: float
 y: float
 z: float


class GpsData(BaseModel):
 latitude: float
 longitude: float

class AgentData(BaseModel):
 accelerometer: AccelerometerData
 gps: GpsData
 timestamp: datetime

 @classmethod
 @field_validator('timestamp', mode='before')
 def check_timestamp(cls, value):
   if isinstance(value, datetime):
     return value
   try:
     return datetime.fromisoformat(value)
   except (TypeError, ValueError):
     raise ValueError(
 "Invalid timestamp format. Expected ISO 8601 format(YYYY-MM-DDTHH:MM:SSZ).")



class ProcessedAgentData(BaseModel):
  road_state: str
  agent_data: AgentData


# Database model
class ProcessedAgentDataInDB(BaseModel):
  id: int
  road_state: str
  x: float
  y: float
  z: float
  latitude: float
  longitude: float
  timestamp: datetime


# FastAPI app setup
app = FastAPI()

# WebSocket subscriptions
subscriptions: Set[WebSocket] = set()

# FastAPI WebSocket endpoint
@app.websocket("/ws/")
async def websocket_endpoint(websocket: WebSocket):
  await websocket.accept()
  subscriptions.add(websocket)
  try:
    while True:
      await websocket.receive_text()
  except WebSocketDisconnect:
    subscriptions.remove(websocket)


# Function to send data to subscribed users
async def send_data_to_subscribers(data):
  for websocket in subscriptions:
    await websocket.send_json(json.dumps(data))


# FastAPI CRUDL endpoints

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/processed_agent_data/")
async def create_processed_agent_data(data:List[ProcessedAgentData], session = Depends(get_db)):
  try:
    for entry in data:
      # Insert data to database
      query = insert(processed_agent_data).values(
        road_state=entry.road_state,
        x=entry.agent_data.accelerometer.x,
        y=entry.agent_data.accelerometer.y,
        z=entry.agent_data.accelerometer.z,
        latitude=entry.agent_data.gps.latitude,
        longitude=entry.agent_data.gps.longitude,
        timestamp=datetime.now()
      )
      session.execute(query)
    session.commit()
    for entry in data:
      # Send data to subscribers
      await send_data_to_subscribers(entry.json())
    return {"message": "processed_agent_data has been successfully added"}
  except Exception as ex:
    session.rollback()
    raise HTTPException(status_code=500, detail=str(ex))

@app.get(
 "/processed_agent_data/{processed_agent_data_id}",
 response_model=ProcessedAgentDataInDB
)
def read_processed_agent_data(processed_agent_data_id: int, session = Depends(get_db)):
  # Get data by id
  query = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
  result = session.execute(query).fetchone()
  if result is None:
      raise HTTPException(status_code=404, detail="id not found")
  return result

@app.get("/processed_agent_data/", response_model=List[ProcessedAgentDataInDB])
def list_processed_agent_data(session = Depends(get_db)):
    # Get list of data
    query = select(processed_agent_data)
    result_set = session.execute(query).fetchall()
    return result_set


@app.put("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def update_processed_agent_data(processed_agent_data_id: int,
                                data: ProcessedAgentData,
                                session = Depends(get_db)):
    try:
        # Update data
        query = (
            update(processed_agent_data)
            .where(processed_agent_data.c.id == processed_agent_data_id)
            .values(
                road_state=data.road_state,
                x=data.agent_data.accelerometer.x,
                y=data.agent_data.accelerometer.y,
                z=data.agent_data.accelerometer.z,
                latitude=data.agent_data.gps.latitude,
                longitude=data.agent_data.gps.longitude,
                timestamp=datetime.now()
            )
        )
        session.execute(query)
        session.commit()
        updated_row = session.execute(select(processed_agent_data)
        .where(
            processed_agent_data.c.id == processed_agent_data_id
        )).fetchone()
        return updated_row
    except Exception as ex:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(ex))

@app.delete("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def delete_processed_agent_data(processed_agent_data_id: int, session = Depends(get_db)):
    try:
        entry = session.execute(
            select(processed_agent_data)
            .where(
                processed_agent_data.c.id == processed_agent_data_id
            )).fetchone()

        if entry is None:
            raise HTTPException(status_code=404, detail="id not found")
        # Delete by id
        query = (delete(processed_agent_data)
        .where(processed_agent_data.c.id == processed_agent_data_id))
        session.execute(query)
        session.commit()
        return entry
    except Exception as ex:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(ex))


if __name__ == "__main__":
  import uvicorn
  uvicorn.run(app, host="127.0.0.1", port=8000)