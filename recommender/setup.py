import azure.functions as func
import azure.durable_functions as df

from recommender.orchestrator import main as orchestrator_bp
from recommender.activities.filter_bci import blueprint as filter_bci_bp
from recommender.activities.deduplicate import blueprint as deduplicate_bp
from recommender.activities.domain_agents import blueprint as domain_agents_bp


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

    # Register the blueprints
    app.register_functions(orchestrator_bp)
    app.register_functions(filter_bci_bp)
    app.register_functions(deduplicate_bp)
    app.register_functions(domain_agents_bp)
