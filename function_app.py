import azure.functions as func
import azure.durable_functions as df
from recommender.setup import register_recommender

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

register_recommender(app)