import azure.functions as func
import azure.durable_functions as df
#from documents.setup import register_documents
#from storage.setup import register_storage
from recommender.setup import register_recommender

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

#register_documents(app)
#register_storage(app)
register_recommender(app)