# lac-attorney


### Setting up `.env`
Create an `.env` file that includes a `GA_BUCKET` key.

```
GA_BUCKET='YOUR_GOOGLE_CLOUD_BUCKET'
```

### Running the job

```
python main.py
```

### Setting up virtualenv, Docker, and Make

#### virtualenv
```
python3 -m venv venv
pip install -r requirements.txt
```

#### Make

Build python
```
make build
```

Use notebook (if necessary)

```
make notebook
```