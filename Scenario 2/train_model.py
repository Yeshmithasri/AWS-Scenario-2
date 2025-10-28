import pandas as pd
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression
import joblib

# Database connection
engine = create_engine("postgresql://smv_user:smv_pass123@smv-postgres.c18qo6wkyztn.eu-north-1.rds.amazonaws.com:5432/smv_db")

# Load processed data
df = pd.read_sql('SELECT * FROM processed_data', engine)

# Train a simple model
X, y = df[['Quantity']], df['Price']
model = LinearRegression().fit(X, y)

# Save trained model
joblib.dump(model, 'smv_predictor_v1.pkl')
print("Model trained and saved as smv_predictor_v1.pkl")
