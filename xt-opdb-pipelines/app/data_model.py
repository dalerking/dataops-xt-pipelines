from pydantic import BaseModel


class JobDetails(BaseModel):
    pipelineName: str


class Data(BaseModel):
    jobDetails: JobDetails
    databaseDetails: dict[str, str]


class Metadata(BaseModel):
    startDate: str
    endDate: str


class Event(BaseModel):
    method: str
    data: Data
    metadata: Metadata
