import azure.functions as func
import azure.durable_functions as df
from recommender.setup import register_recommender

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="hello")
def test_function(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("The backend is alive!", status_code=200)

register_recommender(app)