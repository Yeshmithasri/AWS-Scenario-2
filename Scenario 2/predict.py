import pandas as pd
import joblib

# Load trained model
model = joblib.load('smv_predictor_v1.pkl')
print("Model loaded successfully!")

# Load new data and make predictions
new_data = pd.DataFrame({'Quantity': [2, 5, 10]})
predictions = model.predict(new_data)
new_data['Prediction'] = predictions
new_data.to_csv('predictions.csv', index=False)
print("Predictions saved to predictions.csv")
