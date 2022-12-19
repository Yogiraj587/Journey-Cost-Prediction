from fastapi import FastAPI
import uvicorn
import pickle as pkl

app = FastAPI(debug=True)

origins = [
    "http://localhost",
    "http://localhost:3000",
]

@app.get("/")
def home():
    return {'text':'Journey Price Prediction'}

@app.post("/predict")
def predict(Distance:int, TypeofVehicle:int):
    model = pkl.load(open("journey_model.pkl","rb"))
    prediction = model.predict([[Distance,TypeofVehicle]])
    output = round(prediction[0],2)

    return {"The Journey Price is: {}".format(output)}

if __name__ == "__main__":
    uvicorn.run(app)