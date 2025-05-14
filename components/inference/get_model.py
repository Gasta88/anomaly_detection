from kfp.dsl import component, Artifact, Output, Input, Model
@component(
    base_image="python:3.9",
    packages_to_install=["google-cloud-aiplatform"]
    )
def get_model(
    project_id: str,
    location: str,
    model_name: str,
    model: Output[Artifact]
    ):
    from google.cloud import aiplatform

    aiplatform.init(project=project_id, location=location)
    model = [
        # m.resource_name
        m
        for m in aiplatform.Model.list(order_by="create_time desc")
        if model_name in m.name
    ][0]