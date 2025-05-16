from kfp.dsl import component
@component(
    base_image="python:3.9",
    packages_to_install=["google-cloud-aiplatform"]
    )
def get_model(
    project_id: str,
    location: str,
    model_name: str,
    ) -> str:
    from google.cloud import aiplatform

    aiplatform.init(project=project_id, location=location)
    return [
        m.resource_name
        for m in aiplatform.Model.list(order_by="create_time desc")
        if model_name in m.display_name
    ][0]