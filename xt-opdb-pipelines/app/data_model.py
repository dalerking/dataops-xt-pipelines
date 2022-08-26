from pydantic import BaseModel


class JobDetails(BaseModel):
    pipelineName: str


class Data(BaseModel):
    jobDetails: JobDetails
    databaseDetails: dict[str, str]


class Event(BaseModel):
    data: Data
