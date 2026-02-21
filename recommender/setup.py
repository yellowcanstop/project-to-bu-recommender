import azure.functions as func
import azure.durable_functions as df

from recommender.orchestrator import recommender_orchestrator_function
from recommender.activities.filter_bci import filter_bci_activity
from recommender.activities.deduplicate import deduplicate_activity
from recommender.activities.domain_agents import process_single_lead_activity


def register_recommender(app: df.DFApp):

    @app.route(route="recommender/start", methods=["POST"])
    @app.durable_client_input(client_name="client")
    async def start_recommender(req: func.HttpRequest, client: df.DurableOrchestrationClient):
        body = req.get_json()
        instance_id = await client.start_new("recommender_orchestrator", client_input=body)
        return client.create_check_status_response(req, instance_id)

    # callback for human-approved removal of BCI and non-BCI duplicates
    @app.route(route="recommender/approve/{instance_id}", methods=["POST"])
    @app.durable_client_input(client_name="client")
    async def approve_duplicates(req: func.HttpRequest, client: df.DurableOrchestrationClient):
        instance_id = req.route_params["instance_id"]
        body = req.get_json()  # {"removed_ids": ["MEISID123", ...]}
        await client.raise_event(instance_id, "duplicate_approval", body)
        return func.HttpResponse(status_code=202)

    @app.route(route="recommender/status/{instance_id}", methods=["GET"])
    @app.durable_client_input(client_name="client")
    async def check_status(req: func.HttpRequest, client: df.DurableOrchestrationClient):
        instance_id = req.route_params["instance_id"]
        status = await client.get_status(instance_id)
        return func.HttpResponse(status.to_json(), mimetype="application/json")

    app.register_functions(recommender_orchestrator_function)
    app.register_functions(filter_bci_activity)
    app.register_functions(deduplicate_activity)
    app.register_functions(process_single_lead_activity)
