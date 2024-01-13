from fastapi import FastAPI
import pandas as pd
from fastapi.responses import JSONResponse

app = FastAPI()

# Load the CSV file into a Pandas DataFrame
df = pd.read_csv('powerconsumption.csv') 

@app.get("/")
async def read_sample_row():
    # Sample a row from the DataFrame
    sampled_row = df.sample().to_dict(orient='records')[0]

    # Return the sampled row as JSON response
    return JSONResponse(content=sampled_row)
