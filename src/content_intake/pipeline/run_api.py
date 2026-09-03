# src/content_intake/pipeline/run_api.py
import uvicorn

from content_intake.pipeline.api import create_api_app


def run(port: int = 8090) -> None:
    uvicorn.run(create_api_app(), host="0.0.0.0", port=port, workers=1, log_level="info")


if __name__ == "__main__":
    run()
