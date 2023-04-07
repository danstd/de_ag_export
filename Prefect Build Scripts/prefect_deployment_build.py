# Create prefect deployment.
# https://docs.prefect.io/latest/concepts/deployments/#create-a-deployment-from-a-python-object
from de_ag_pipeline import de_ag_flow
from prefect.deployments import Deployment

deployment = Deployment.build_from_flow(
    flow=de_ag_flow,
    name="de_ag_deployment", 
    version=1, 
    work_queue_name="de_ag_queue",
)
deployment.apply()