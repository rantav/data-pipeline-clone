# data-pipeline

## Install: 
1. Install python **poetry**
1. Then run:

```sh
poetry install
```
## Run

To test installation: 
```sh
poetry run prefect version
```

To run a local dev prefect server: 
```sh
poetry run prefect server start
```

To run a flow as standalone locally: 
```sh
poetry run python video/video_flow.py 
```