# lac-attorney

### Setting up
```
# clone repo
git clone https://github.com/kvn219/lac-attorney.git

# move into the lac-attorney directory
cd lac-attorney

# create .env file
echo GA_BUCKET='YOUR_GOOGLE_CLOUD_BUCKET'  > .env

# setup virtualenv
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# setup docker
make build
make notebook

# deactivate virtualenv
deactivate
```

Tested with `Python 3.9.1`.